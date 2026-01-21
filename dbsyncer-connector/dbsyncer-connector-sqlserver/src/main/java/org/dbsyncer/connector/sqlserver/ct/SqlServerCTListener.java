package org.dbsyncer.connector.sqlserver.ct;

import org.apache.commons.codec.digest.DigestUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.ct.model.CTEvent;
import org.dbsyncer.connector.sqlserver.ct.model.DDLChange;
import org.dbsyncer.connector.sqlserver.ct.model.DDLChangeType;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * SQL Server Change Tracking (CT) 监听器实现
 * 使用 Change Tracking 替代 CDC
 */
public class SqlServerCTListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // Snapshot 键名
    private static final String VERSION_POSITION = "version";

    // 流式查询的 fetchSize，控制每次从数据库获取的记录数
    private static final int STREAMING_FETCH_SIZE = 5000;

    private final SqlServerTemplate sqlTemplate;

    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Set<String> tables;
    private DatabaseConnectorInstance instance;
    private SqlServerCTQueryUtil queryUtil;
    private Worker worker;
    private Long lastVersion;
    private String serverName;
    private String schema;
    private String realDatabaseName;
    // 版本号轮询间隔（毫秒）
    private static final long POLL_INTERVAL_MILLIS = 100;
    // 主键信息缓存（表名 -> 主键列表）
    private final Map<String, List<String>> primaryKeysCache = new HashMap<>();
    // 表列数缓存（表名 -> 列数），避免重复查询 INFORMATION_SCHEMA
    private final Map<String, Integer> tableColumnCountCache = new HashMap<>();
    // Schema 信息内存缓存（表名 -> MetaInfo）
    private final Map<String, MetaInfo> schemaCache = new ConcurrentHashMap<>();
    // 列位置映射内存缓存（表名 -> Map<列名, 位置>）
    private final Map<String, Map<String, Integer>> ordinalPositionsCache = new ConcurrentHashMap<>();
    // Schema 哈希值内存缓存（表名 -> 哈希值）
    private final Map<String, String> schemaHashCache = new ConcurrentHashMap<>();

    /**
     * 构造函数
     *
     * @param sqlTemplate SQL Server 模板实例，用于构建 SQL 语句
     */
    public SqlServerCTListener(SqlServerTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }

    @Override
    public void start() throws Exception {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("SqlServerCTListener is already started");
                return;
            }
            connected = true;
            connect();
            readTables();
            Assert.notEmpty(tables, "No tables available");

            enableDBChangeTracking();
            enableTableChangeTracking();
            readLastVersion();

            // 启动时初始化所有表的 schema 缓存
            initializeSchemaCache();

            worker = new Worker();
            worker.setName(new StringBuilder("ct-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            close();
            logger.error("启动失败: {}", e.getMessage(), e);
            throw new SqlServerException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        if (connected) {
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
            connected = false;
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (offset.getPosition() != null) {
            snapshot.put(VERSION_POSITION, offset.getPosition().toString());
            super.refreshEvent(offset);
        }
    }

    public Long getMaxVersion() throws Exception {
        return queryUtil.queryAndMap(sqlTemplate.buildGetCurrentVersionSql(), rs -> {
            Long version = rs.getLong(1);
            return rs.wasNull() ? null : version;
        });
    }

    public Long getLastVersion() {
        return lastVersion;
    }


    private void connect() throws Exception {
        instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;
        if (service.isAlive(instance)) {
            DatabaseConfig cfg = instance.getConfig();
            serverName = cfg.getUrl();
            schema = cfg.getSchema();
            if (schema == null || schema.isEmpty()) {
                schema = "dbo";
            }
            // 初始化查询工具类
            queryUtil = new SqlServerCTQueryUtil(instance);
        }
    }

    private void readLastVersion() throws Exception {
        if (!snapshot.containsKey(VERSION_POSITION)) {
            lastVersion = getMaxVersion();
            if (lastVersion != null) {
                snapshot.put(VERSION_POSITION, String.valueOf(lastVersion));
                return;
            }
            throw new SqlServerException("No Change Tracking version available");
        }
        lastVersion = Long.valueOf(snapshot.get(VERSION_POSITION));
    }

    private void readTables() throws Exception {
        String sql = sqlTemplate.buildGetTableListSql(schema);
        tables = queryUtil.queryAndMapList(sql, rs -> {
            Set<String> tableSet = new LinkedHashSet<>();
            while (rs.next()) {
                String tableName = rs.getString(1);
                if (filterTable.contains(tableName)) {
                    tableSet.add(tableName);
                }
            }
            return tableSet;
        });
    }

    private void enableDBChangeTracking() throws Exception {
        realDatabaseName = queryUtil.queryAndMap(sqlTemplate.buildGetDatabaseNameSql(), rs -> rs.getString(1));
        Integer count = queryUtil.queryAndMap(sqlTemplate.buildIsDatabaseChangeTrackingEnabledSql(realDatabaseName), rs -> rs.getInt(1));
        if (count == null || count == 0) {
            execute(sqlTemplate.buildEnableDatabaseChangeTrackingSql(realDatabaseName));
            logger.info("已启用数据库 [{}] 的 Change Tracking", realDatabaseName);
        }
    }

    private void enableTableChangeTracking() {
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        tables.forEach(table -> {
            try {
                String checkSql = sqlTemplate.buildIsTableChangeTrackingEnabledSql(schema, table);
                Integer count = queryUtil.query(checkSql, null, rs -> {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                    return 0;
                });
                if (count == null || count == 0) {
                    try {
                        execute(sqlTemplate.buildEnableTableChangeTrackingSql(schema, table));
                        logger.info("已启用表 [{}].[{}] 的 Change Tracking", schema, table);
                    } catch (UncategorizedSQLException e) {
                        logger.warn("表 [{}] 的 Change Tracking 可能已存在", table);
                    } catch (Exception e) {
                        logger.error("启用表 [{}] 的 Change Tracking 失败: {}", table, e.getMessage(), e);
                        throw new RuntimeException("启用表 [" + table + "] 的 Change Tracking 失败", e);
                    }
                }
            } catch (Exception e) {
                logger.error("检查表 [{}] 的 Change Tracking 状态失败: {}", table, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    private void execute(String... sqlStatements) throws Exception {
        instance.execute(databaseTemplate -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    logger.info("执行 SQL: {}", sqlStatement);
                    databaseTemplate.execute(sqlStatement);
                }
            }
            return true;
        });
    }

    /**
     * 流式处理 DML 变更，边查询边发送事件，避免内存溢出
     * DDL 变更在查询 DML 时检测到后立即发送
     * 所有事件都携带 startVersion，确保数据安全（即使处理失败也不会丢失数据）
     *
     * @param startVersion 起始版本号
     * @param stopVersion  结束版本号
     */
    private void pull(Long startVersion, Long stopVersion) throws Exception {
        // 流式处理每个表的 DML 变更，直接发送，不需要队列
        // 所有事件都携带 startVersion，确保快照持久化使用 startVersion，避免数据丢失
        for (String table : tables) {
            queryDMLChangesWithStreamingAndSend(
                    table, startVersion, stopVersion, primaryKeysCache.get(table),
                    startVersion);
        }

        // 注意：版本号的持久化通过每个事件的 refreshEvent 完成，使用 startVersion
        // 这里只更新内存中的 lastVersion，用于下次查询的起始版本号
        lastVersion = stopVersion;
    }

    /**
     * 流式查询 DML 变更并立即发送事件（真正的流式处理）
     * 所有事件都携带 startVersion，确保数据安全
     *
     * @param tableName    表名
     * @param startVersion 起始版本号
     * @param stopVersion  结束版本号
     * @param primaryKeys  主键列表
     * @param eventVersion 事件版本号（所有事件都使用 startVersion）
     * @return 处理的记录数
     */
    private int queryDMLChangesWithStreamingAndSend(String tableName, Long startVersion, Long stopVersion,
                                                    List<String> primaryKeys,
                                                    Long eventVersion) throws Exception {
        // 验证主键
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new SqlServerException("表 " + tableName + " 没有主键，无法使用 Change Tracking");
        }

        // 构建查询 SQL
        String schemaInfoSubquery = sqlTemplate.buildGetTableSchemaInfoSubquery(schema, tableName);
        String mainQuery = sqlTemplate.buildChangeTrackingDMLMainQuery(schema, tableName, primaryKeys, schemaInfoSubquery);
        String fallbackQuery = sqlTemplate.buildSchemeOnly(schema, tableName);

        // 使用流式查询，边查询边发送
        return instance.execute(databaseTemplate -> {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                // 使用流式结果集
                ps = databaseTemplate.getSimpleConnection().prepareStatement(
                        mainQuery,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY
                );
                ps.setFetchSize(STREAMING_FETCH_SIZE);
                ps.setLong(1, startVersion);
                ps.setLong(2, startVersion);
                ps.setLong(3, stopVersion);

                rs = ps.executeQuery();

                // 处理查询结果
                return processDMLResultSet(rs, tableName, primaryKeys, eventVersion, startVersion, stopVersion, fallbackQuery);
            } catch (Exception e) {
                logger.error("流式查询 DML 变更失败，表: {}, SQL: {}, 错误: {}", tableName, mainQuery, e.getMessage(), e);
                throw e;
            } finally {
                rs.close();
                ps.close();
            }
        });
    }

    /**
     * 处理 DML 查询结果集，流式处理并发送事件
     *
     * @param rs            结果集
     * @param tableName     表名
     * @param primaryKeys   主键列表
     * @param eventVersion  事件版本号（所有事件都使用 startVersion）
     * @param startVersion  起始版本号
     * @param stopVersion   结束版本号
     * @param fallbackQuery 辅助查询 SQL（用于检测 DDL）
     * @return 处理的记录数
     * @throws Exception 处理异常
     */
    private int processDMLResultSet(ResultSet rs, String tableName, List<String> primaryKeys,
                                    Long eventVersion, Long startVersion, Long stopVersion,
                                    String fallbackQuery) throws Exception {
        ResultSetMetaData metaData = rs.getMetaData();

        // 查找表结构信息列的位置
        int schemaInfoColumnIndex = -1;
        int resultColumnCount = metaData.getColumnCount();
        for (int i = 1; i <= resultColumnCount; i++) {
            if (SqlServerTemplate.CT_DDL_SCHEMA_INFO_COLUMN.equalsIgnoreCase(metaData.getColumnName(i))) {
                schemaInfoColumnIndex = i;
                break;
            }
        }

        // 检测 DDL 变更（从第一行获取）
        // 注意：DDL 检测是在处理 DML 查询的第一行时进行的，此时还没有处理任何 DML 事件
        // 所以可以立即发送 DDL，然后再处理 DML，保证 DDL 在使用新结构的 DML 之前处理
        boolean hasData = false;
        if (schemaInfoColumnIndex > 0 && rs.next()) {
            hasData = true;
            String schemaInfoJson = rs.getString(schemaInfoColumnIndex);
            // 检测到 DDL 时立即发送，不使用版本号（因为 DDL 是检测生成的，没有实际版本号）
            detectDDLChangesFromSchemaInfoJsonAndSendImmediately(tableName, schemaInfoJson, String.valueOf(startVersion));
        }

        // 构建列映射
        Map<String, Integer> primaryKeyToCTIndex = new HashMap<>();
        int ctPkStartIndex = 4;
        for (int i = 0; i < primaryKeys.size(); i++) {
            primaryKeyToCTIndex.put(primaryKeys.get(i), ctPkStartIndex + i);
        }

        int tStarStartIndex = 4 + primaryKeys.size();
        int tStarEndIndex = schemaInfoColumnIndex > 0 ? schemaInfoColumnIndex - 1 : resultColumnCount - 1;

        Set<Integer> columnsToSkip = new HashSet<>();
        if (schemaInfoColumnIndex > 0) {
            columnsToSkip.add(schemaInfoColumnIndex);
        }

        List<String> columnNames = new ArrayList<>();
        Map<Integer, String> columnIndexToName = new HashMap<>();
        Set<String> primaryKeySet = new HashSet<>(primaryKeys);
        for (int i = tStarStartIndex; i <= tStarEndIndex; i++) {
            if (columnsToSkip.contains(i)) {
                continue;
            }
            String columnName = metaData.getColumnName(i);
            columnNames.add(columnName);
            columnIndexToName.put(i, columnName);
        }

        int processedCount = 0;

        // 处理第一行（如果已读取）
        if (hasData) {
            CTEvent event = processRow(rs, tableName, tStarStartIndex, tStarEndIndex, columnsToSkip,
                    columnIndexToName, columnNames, primaryKeySet, primaryKeyToCTIndex);
            if (event != null) {
                sendDMLEvent(event, eventVersion);
                processedCount++;
            }
        }

        // 流式处理后续行：边查询边发送
        while (rs.next()) {
            CTEvent event = processRow(rs, tableName, tStarStartIndex, tStarEndIndex, columnsToSkip,
                    columnIndexToName, columnNames, primaryKeySet, primaryKeyToCTIndex);
            if (event != null) {
                sendDMLEvent(event, eventVersion);
                processedCount++;
            }
        }

        // 如果没有 DML 数据，执行辅助查询检测 DDL
        if (processedCount == 0) {
            queryUtil.queryAndMapList(fallbackQuery, rs2 -> {
                if (rs2.next()) {
                    String schemaInfoJson = rs2.getString("schema_info");
                    try {
                        // 检测到 DDL 时立即发送，不使用版本号
                        detectDDLChangesFromSchemaInfoJsonAndSendImmediately(tableName, schemaInfoJson, String.valueOf(startVersion));
                    } catch (Exception e) {
                        logger.error("检测 DDL 变更失败: {}", e.getMessage(), e);
                    }
                }
                return null;
            });
        }

        return processedCount;
    }

    /**
     * 发送 DML 事件
     * 所有事件都携带 startVersion，确保快照持久化使用 startVersion，避免数据丢失
     *
     * @param event        DML 事件
     * @param eventVersion 事件版本号（所有事件都使用 startVersion）
     */
    private void sendDMLEvent(CTEvent event, Long eventVersion) {
        RowChangedEvent rowEvent = new RowChangedEvent(
                event.getTableName(),
                event.getCode(),
                event.getRow(),
                null,
                eventVersion,  // 所有事件都使用 startVersion
                event.getColumnNames()
        );
        trySendEvent(rowEvent);
    }


    // ==================== 辅助方法 ====================


    /**
     * 处理单行数据，构建 CTEvent
     * 性能优化：使用预构建的列名列表和映射，避免重复调用 getColumnName
     *
     * @param rs                  ResultSet（当前行已定位）
     * @param tableName           表名
     * @param tStarStartIndex     T.* 列的起始索引
     * @param tStarEndIndex       T.* 列的结束索引
     * @param columnsToSkip       需要跳过的列索引集合
     * @param columnIndexToName   列索引到列名的映射（预构建）
     * @param columnNames         预构建的列名列表（所有行的列名都相同）
     * @param primaryKeySet       主键集合（使用 Set 提高查找效率）
     * @param primaryKeyToCTIndex 主键列名到 CT 主键列索引的映射
     * @return CTEvent 对象，如果处理失败返回 null
     */
    private CTEvent processRow(ResultSet rs, String tableName, int tStarStartIndex, int tStarEndIndex,
                               Set<Integer> columnsToSkip, Map<Integer, String> columnIndexToName,
                               List<String> columnNames, Set<String> primaryKeySet, Map<String, Integer> primaryKeyToCTIndex) {
        try {
            Long version = rs.getLong(1);
            String operation = rs.getString(2);  // 'I', 'U', 'D'
            // 跳过 SYS_CHANGE_COLUMNS（第 3 列），不使用

            // 构建行数据（列名列表已预构建，直接使用）
            List<Object> row = new ArrayList<>();
            for (int i = tStarStartIndex; i <= tStarEndIndex; i++) {
                if (columnsToSkip.contains(i)) {
                    continue;
                }

                Object value = rs.getObject(i);
                String columnName = columnIndexToName.get(i);

                // 如果是主键列且值为 NULL（DELETE 情况），使用冗余的 CT_[pk] 值
                // CT 主键列在固定位置（4 到 4+PK-1），可以直接通过映射获取索引
                if (primaryKeySet.contains(columnName) && value == null) {
                    Integer ctPkIndex = primaryKeyToCTIndex.get(columnName);
                    if (ctPkIndex != null) {
                        value = rs.getObject(ctPkIndex);
                    }
                }

                row.add(value);
            }

            // 转换操作类型
            String operationCode = convertOperation(operation);

            // 使用预构建的列名列表（所有行的列名都相同）
            return new CTEvent(tableName, operationCode, row, version, columnNames);
        } catch (SQLException e) {
            logger.error("处理行数据失败: {}", e.getMessage(), e);
            return null;
        }
    }

    private String convertOperation(String operation) {
        if ("I".equals(operation)) {
            return ConnectorConstant.OPERTION_INSERT;
        } else if ("U".equals(operation)) {
            return ConnectorConstant.OPERTION_UPDATE;
        } else if ("D".equals(operation)) {
            return ConnectorConstant.OPERTION_DELETE;
        }
        throw new IllegalArgumentException("Unknown operation: " + operation);
    }

    private Long getCurrentVersion() throws Exception {
        return queryUtil.queryAndMap(sqlTemplate.buildGetCurrentVersionSql(), rs -> {
            Long version = rs.getLong(1);
            return rs.wasNull() ? null : version;
        });
    }

    /**
     * 内部类：用于存储表的主键信息和列数
     */
    private static class TableMetaInfo {
        final List<String> primaryKeys;
        final int columnCount;

        TableMetaInfo(List<String> primaryKeys, int columnCount) {
            this.primaryKeys = primaryKeys;
            this.columnCount = columnCount;
        }
    }

    /**
     * 合并查询：同时获取主键信息和列数（减少查询次数）
     */
    private TableMetaInfo getPrimaryKeysAndColumnCount(String tableName) throws Exception {
        String sql = sqlTemplate.buildGetPrimaryKeysAndColumnCountSql(schema, tableName);
        // 使用 READ UNCOMMITTED 隔离级别避免死锁
        // 注意：优化后的 SQL 使用 CROSS APPLY，只有 2 个参数占位符（在 CROSS APPLY 子查询中）
        // 主查询的 WHERE 条件已通过字符串格式化处理，不再需要参数
        return queryUtil.queryWithReadUncommitted(sql, statement -> {
            statement.setString(1, schema);  // CROSS APPLY 子查询的 TABLE_SCHEMA
            statement.setString(2, tableName);  // CROSS APPLY 子查询的 TABLE_NAME
        }, rs -> {
            List<String> pks = new ArrayList<>();
            int columnCount = 0;
            boolean columnCountSet = false;

            while (rs.next()) {
                pks.add(rs.getString("COLUMN_NAME"));
                // 列数在所有行中都相同，只需要读取一次
                if (!columnCountSet) {
                    columnCount = rs.getInt("COLUMN_COUNT");
                    columnCountSet = true;
                }
            }

            // 如果没有主键（pks 为空），仍然需要获取列数
            // 注意：这种情况不应该发生（因为主键是必需的），但为了健壮性仍然处理
            if (!columnCountSet) {
                try {
                    columnCount = queryUtil.queryAndMapList(
                            String.format("SELECT COUNT(*) AS col_count FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                                    schema, tableName),
                            rs2 -> {
                                if (rs2.next()) {
                                    return rs2.getInt("col_count");
                                }
                                return 0;
                            }
                    );
                } catch (Exception e) {
                    logger.warn("获取表 {} 的列数失败，使用默认值 0: {}", tableName, e.getMessage());
                    columnCount = 0;
                }
            }

            return new TableMetaInfo(pks, columnCount);
        });
    }

    private List<String> getPrimaryKeysFromMetaInfo(MetaInfo metaInfo) {
        if (metaInfo == null || metaInfo.getColumn() == null) {
            return new ArrayList<>();
        }
        return metaInfo.getColumn().stream()
                .filter(Field::isPk)
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    // ==================== DDL 检测相关方法 ====================

    /**
     * 从 JSON 字段中的表结构信息检测 DDL 变更并立即发送
     * DDL 事件不使用版本号，因为 DDL 是检测生成的，没有实际版本号意义
     * 检测到 DDL 时立即发送，保证在使用新结构的 DML 之前处理
     *
     * @param tableName      表名
     * @param schemaInfoJson 表结构信息的 JSON 字符串（来自 INFORMATION_SCHEMA.COLUMNS）
     */
    private void detectDDLChangesFromSchemaInfoJsonAndSendImmediately(String tableName, String schemaInfoJson, String version) throws Exception {
        if (schemaInfoJson == null || schemaInfoJson.trim().isEmpty()) {
            logger.warn("表 {} 的表结构信息 JSON 为空，跳过 DDL 检测", tableName);
            return;
        }

        // 1. 直接基于 JSON 字符串计算哈希值（快速，避免解析 JSON 的开销）
        String currentHash = DigestUtils.md5Hex(schemaInfoJson);
        String lastHash = schemaHashCache.get(tableName);

        if (lastHash == null) {
            // 首次检测（内存缓存中没有旧的哈希值），直接更新内存缓存，不进行 DDL 变更检测
            // 因为无法知道是否有变更（没有旧的 schema 信息进行比对）
            // 注意：正常情况下不会发生，因为启动时已初始化；但如果初始化失败或缓存丢失，这里会重新初始化
            Long detectedVersion = getCurrentVersion();
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, detectedVersion, currentHash, ordinalPositions);

            // 首次检测时也更新缓存（如果缓存为空）
            if (primaryKeysCache.get(tableName) == null || tableColumnCountCache.get(tableName) == null) {
                MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
                List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
                int currentColumnCount = ordinalPositions.size();
                primaryKeysCache.put(tableName, currentPrimaryKeys);
                tableColumnCountCache.put(tableName, currentColumnCount);
                logger.debug("首次检测表 {} 时更新缓存：主键={}, 列数={}", tableName, currentPrimaryKeys, currentColumnCount);
            }
            logger.debug("表 {} 首次检测，更新内存缓存，不进行 DDL 变更检测", tableName);
            return;
        }
        if (lastHash.equals(currentHash)) {
            return; // 哈希值未变化，无 DDL 变更（避免解析 JSON 的开销）
        }

        // 2. 哈希值变化，解析 JSON 构建 MetaInfo 进行详细比对
        MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
        MetaInfo lastMetaInfo = schemaCache.get(tableName);

        if (lastMetaInfo == null) {
            // 内存缓存中没有旧的 schema（正常情况下不会发生，因为启动时已初始化）
            logger.warn("表 {} 的 DDL 变更检测：内存缓存中没有旧的 schema，无法进行详细比对，直接更新内存缓存", tableName);

            // 更新内存缓存（使用检测时的版本号）
            Long detectedVersion = getCurrentVersion();
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, detectedVersion, currentHash, ordinalPositions);
            return;
        }

        List<String> lastPrimaryKeys = getPrimaryKeysFromMetaInfo(lastMetaInfo);
        List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
        Map<String, Integer> lastOrdinalPositions = getColumnOrdinalPositions(tableName);
        Map<String, Integer> currentOrdinalPositions = extractOrdinalPositions(schemaInfoJson);

        // 3. 比对差异
        logger.info("开始比对表 {} 的 DDL 变更: 旧列数={}, 新列数={}",
                tableName, lastMetaInfo.getColumn().size(), currentMetaInfo.getColumn().size());
        List<DDLChange> changes = compareTableSchema(tableName, lastMetaInfo, currentMetaInfo,
                lastPrimaryKeys, currentPrimaryKeys,
                lastOrdinalPositions, currentOrdinalPositions);
        logger.info("比对完成，检测到 {} 个 DDL 变更", changes.size());

        // 4. 如果有变更，立即发送 DDL 事件（不使用版本号）
        if (changes.isEmpty()) {
            // 即使没有检测到变更，也更新内存缓存（可能是精度/尺寸变化导致哈希变化但无法检测）
            Long detectedVersion = getCurrentVersion();
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, detectedVersion, currentHash, ordinalPositions);
            return;
        }

        // 立即发送 DDL 事件（不使用版本号，因为 DDL 是检测生成的，没有实际版本号意义）
        // DDL 检测是在处理 DML 查询的第一行时进行的，此时还没有处理任何 DML 事件
        // 所以可以立即发送 DDL，然后再处理 DML，保证 DDL 在使用新结构的 DML 之前处理
        for (DDLChange change : changes) {
            DDLChangedEvent ddlEvent = new DDLChangedEvent(
                    tableName,
                    ConnectorConstant.OPERTION_ALTER,
                    change.getDdlCommand(),
                    version,
                    null  // DDL 事件不使用版本号，因为 DDL 是检测生成的，没有实际版本号意义
            );
            trySendEvent(ddlEvent);
            logger.info("检测到表 {} 的 DDL 变更并立即发送: {}", tableName, change.getDdlCommand());
        }

        // 5. 更新表结构快照（直接保存 JSON，避免解析）
        Long detectedVersion = getCurrentVersion();
        Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
        saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, detectedVersion, currentHash, ordinalPositions);

        // 更新主键和字段
        primaryKeysCache.put(tableName, currentPrimaryKeys);
        int currentColumnCount = extractOrdinalPositions(schemaInfoJson).size();
        tableColumnCountCache.put(tableName, currentColumnCount);

        logger.info("检测到表 {} 的 DDL 变更，共 {} 个变更", tableName, changes.size());
    }

    /**
     * 解析 JSON 字符串构建 MetaInfo 对象
     *
     * @param tableName      表名
     * @param schemaInfoJson 表结构信息的 JSON 字符串
     * @return MetaInfo 对象
     */
    private MetaInfo parseSchemaInfoJson(String tableName, String schemaInfoJson) throws Exception {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columnsJson = (List<Map<String, Object>>) JsonUtil.jsonToObj(schemaInfoJson, List.class);
        if (columnsJson == null || columnsJson.isEmpty()) {
            throw new IllegalArgumentException("表 " + tableName + " 的表结构信息 JSON 为空");
        }

        List<Field> columns = new ArrayList<>();

        // 从内存缓存中获取主键信息（避免递归调用 loadTableSchemaSnapshot）
        // 注意：不能调用 loadTableSchemaSnapshot，因为它会调用 parseSchemaInfoJson，导致无限递归
        List<String> primaryKeys = primaryKeysCache.get(tableName);

        // 如果缓存中没有主键信息，则查询数据库
        // 使用合并查询获取主键和列数（即使这里只需要主键，也使用合并查询保持一致性）
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            TableMetaInfo metaInfo = getPrimaryKeysAndColumnCount(tableName);
            primaryKeys = metaInfo.primaryKeys;
            // 更新缓存
            primaryKeysCache.put(tableName, primaryKeys);
            // 同时更新列数缓存（虽然这里不需要，但可以避免后续查询）
            tableColumnCountCache.put(tableName, metaInfo.columnCount);
        }

        for (Map<String, Object> colJson : columnsJson) {
            String columnName = (String) colJson.get("COLUMN_NAME");
            String dataType = (String) colJson.get("DATA_TYPE");
            Object maxLengthObj = colJson.get("CHARACTER_MAXIMUM_LENGTH");
            Object precisionObj = colJson.get("NUMERIC_PRECISION");
            Object scaleObj = colJson.get("NUMERIC_SCALE");
            String isNullableStr = (String) colJson.get("IS_NULLABLE");

            Integer maxLength = maxLengthObj != null ? ((Number) maxLengthObj).intValue() : null;
            Integer precision = precisionObj != null ? ((Number) precisionObj).intValue() : null;
            Integer scale = scaleObj != null ? ((Number) scaleObj).intValue() : null;
            // 解析可空性：IS_NULLABLE 在 SQL Server 中为 "YES" 或 "NO"
            // 如果 isNullableStr 为 null，默认设置为 true（可空），避免误判
            Boolean nullable = (isNullableStr == null || isNullableStr.trim().isEmpty())
                    ? true
                    : "YES".equalsIgnoreCase(isNullableStr.trim());

            Field col = new Field();
            col.setName(columnName);
            col.setTypeName(dataType);
            col.setColumnSize(maxLength != null ? maxLength : (precision != null ? precision : 0));
            col.setRatio(scale != null ? scale : 0);
            col.setNullable(nullable);
            col.setPk(primaryKeys.contains(columnName));

            columns.add(col);
        }

        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setColumn(columns);
        return metaInfo;
    }

    /**
     * 从 JSON 字符串中提取列的位置信息
     *
     * @param schemaInfoJson 表结构信息的 JSON 字符串
     * @return 列名到位置的映射
     */
    private Map<String, Integer> extractOrdinalPositions(String schemaInfoJson) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columnsJson = (List<Map<String, Object>>) JsonUtil.jsonToObj(schemaInfoJson, List.class);
        Map<String, Integer> ordinalPositions = new HashMap<>();
        if (columnsJson != null) {
            for (Map<String, Object> colJson : columnsJson) {
                String columnName = (String) colJson.get("COLUMN_NAME");
                Integer ordinalPosition = ((Number) colJson.get("ORDINAL_POSITION")).intValue();
                ordinalPositions.put(columnName, ordinalPosition);
            }
        }
        return ordinalPositions;
    }

    private List<DDLChange> compareTableSchema(String tableName, MetaInfo oldMetaInfo, MetaInfo newMetaInfo,
                                               List<String> oldPrimaryKeys, List<String> newPrimaryKeys,
                                               Map<String, Integer> oldOrdinalPositions, Map<String, Integer> newOrdinalPositions) {
        List<DDLChange> changes = new ArrayList<>();

        // 构建列映射
        Map<String, Field> oldColumns = oldMetaInfo.getColumn().stream()
                .collect(Collectors.toMap(Field::getName, c -> c));
        Map<String, Field> newColumns = newMetaInfo.getColumn().stream()
                .collect(Collectors.toMap(Field::getName, c -> c));

        // 1. 找出被删除的列和新增的列（用于 RENAME 检测）
        List<Field> droppedColumns = new ArrayList<>();
        List<Field> addedColumns = new ArrayList<>();

        for (Field oldCol : oldMetaInfo.getColumn()) {
            if (!newColumns.containsKey(oldCol.getName())) {
                droppedColumns.add(oldCol);
            }
        }

        for (Field newCol : newMetaInfo.getColumn()) {
            if (!oldColumns.containsKey(newCol.getName())) {
                addedColumns.add(newCol);
            }
        }

        // 2. 检测 RENAME：匹配被删除的列和新增的列（属性相同）
        Set<String> matchedOldNames = new HashSet<>();
        Set<String> matchedNewNames = new HashSet<>();

        for (Field droppedCol : droppedColumns) {
            Field bestMatch = null;
            int minPositionDiff = Integer.MAX_VALUE;

            // 查找属性相同的新列（优先匹配位置最接近的）
            for (Field addedCol : addedColumns) {
                if (!matchedNewNames.contains(addedCol.getName())
                        && isColumnEqualIgnoreName(droppedCol, addedCol)) {
                    // 计算位置差（用于优化匹配）
                    int oldPos = oldOrdinalPositions.getOrDefault(droppedCol.getName(), 0);
                    int newPos = newOrdinalPositions.getOrDefault(addedCol.getName(), 0);
                    int diff = Math.abs(oldPos - newPos);

                    if (diff < minPositionDiff) {
                        minPositionDiff = diff;
                        bestMatch = addedCol;
                    }
                }
            }

            if (bestMatch != null) {
                // 找到匹配：这是 RENAME 操作
                // 使用标准的 CHANGE COLUMN 语法（类似 MySQL），便于 JSQLParser 解析
                // DDL 解析器会自动转换为目标数据库的语法（如同构 SQL Server 的 sp_rename）
                String ddl = generateRenameColumnDDL(tableName, droppedCol, bestMatch);
                changes.add(new DDLChange(DDLChangeType.RENAME_COLUMN, ddl, bestMatch.getName()));
                matchedOldNames.add(droppedCol.getName());
                matchedNewNames.add(bestMatch.getName());
            }
        }

        // 3. 处理剩余的 DROP（真正的删除）
        for (Field droppedCol : droppedColumns) {
            if (!matchedOldNames.contains(droppedCol.getName())) {
                String ddl = generateDropColumnDDL(tableName, droppedCol);
                changes.add(new DDLChange(DDLChangeType.DROP_COLUMN, ddl, droppedCol.getName()));
            }
        }

        // 4. 处理剩余的 ADD（真正的新增）
        for (Field addedCol : addedColumns) {
            if (!matchedNewNames.contains(addedCol.getName())) {
                String ddl = generateAddColumnDDL(tableName, addedCol);
                changes.add(new DDLChange(DDLChangeType.ADD_COLUMN, ddl, addedCol.getName()));
            }
        }

        // 5. 检测修改列（类型、长度、精度、可空性）
        for (Field newCol : newMetaInfo.getColumn()) {
            Field oldCol = oldColumns.get(newCol.getName());
            if (oldCol != null) {
                boolean isEqual = isColumnEqual(oldCol, newCol);
                logger.debug("比对列属性: 表={}, 列={}, 是否相等={}, 旧可空性={}, 新可空性={}, 旧类型={}, 新类型={}, 旧长度={}, 新长度={}",
                        tableName, newCol.getName(), isEqual,
                        oldCol.getNullable(), newCol.getNullable(),
                        oldCol.getTypeName(), newCol.getTypeName(),
                        oldCol.getColumnSize(), newCol.getColumnSize());
                if (!isEqual) {
                    // 列属性变更
                    logger.info("检测到列属性变更: 表={}, 列={}, 旧可空性={}, 新可空性={}",
                            tableName, newCol.getName(), oldCol.getNullable(), newCol.getNullable());
                    String ddl = generateAlterColumnDDL(tableName, newCol);
                    logger.info("生成 ALTER COLUMN DDL: {}", ddl);
                    changes.add(new DDLChange(DDLChangeType.ALTER_COLUMN, ddl, newCol.getName()));
                }
            }
        }

        // 6. 检测主键变更（可选，通常不常见）
        if (!oldPrimaryKeys.equals(newPrimaryKeys)) {
            String ddl = generateAlterPrimaryKeyDDL(tableName, oldPrimaryKeys, newPrimaryKeys);
            changes.add(new DDLChange(DDLChangeType.ALTER_PRIMARY_KEY, ddl, null));
        }

        return changes;
    }

    private boolean isColumnEqual(Field oldCol, Field newCol) {
        return Objects.equals(oldCol.getTypeName(), newCol.getTypeName())
                && Objects.equals(oldCol.getColumnSize(), newCol.getColumnSize())
                && Objects.equals(oldCol.getRatio(), newCol.getRatio())
                && Objects.equals(oldCol.getNullable(), newCol.getNullable());
    }

    /**
     * 判断两个列是否相等（忽略列名）
     * 比较：类型、长度、精度
     * 注意：忽略 nullable 和 DEFAULT 值的比较，因为：
     * 1. SQL Server 在执行 sp_rename 后，列的 nullable 属性可能发生变化
     * 2. 快照保存/加载时，nullable 可能为 null（JSON 序列化/反序列化问题）
     * 3. DEFAULT 值不影响 RENAME 识别
     * 示例：first_name (nullable=null) vs full_name (nullable=false) 应该识别为 RENAME
     */
    private boolean isColumnEqualIgnoreName(Field col1, Field col2) {
        return Objects.equals(col1.getTypeName(), col2.getTypeName())
                && Objects.equals(col1.getColumnSize(), col2.getColumnSize())
                && Objects.equals(col1.getRatio(), col2.getRatio());
    }

    private String generateAddColumnDDL(String tableName, Field column) {
        StringBuilder ddl = new StringBuilder();
        // 生成标准格式的 DDL（不带方括号），与 CDC 方式保持一致，便于 JSQLParser 解析
        if (schema != null && !schema.trim().isEmpty()) {
            ddl.append("ALTER TABLE ").append(schema).append(".").append(tableName).append(" ");
        } else {
            ddl.append("ALTER TABLE ").append(tableName).append(" ");
        }
        ddl.append("ADD ").append(column.getName()).append(" ");
        ddl.append(column.getTypeName());

        // 处理长度/精度
        String typeName = column.getTypeName().toLowerCase();
        if (column.getColumnSize() > 0) {
            if (typeName.contains("varchar") || typeName.contains("char") || typeName.contains("varbinary") || typeName.contains("binary")) {
                ddl.append("(").append(column.getColumnSize()).append(")");
            }
        } else if (column.getColumnSize() == -1) {
            // SQL Server 中 -1 表示 MAX 长度（VARCHAR(MAX), NVARCHAR(MAX), VARBINARY(MAX)）
            if (typeName.contains("varchar") || typeName.contains("varbinary")) {
                ddl.append("(MAX)");
            }
        }

        // 处理数值类型的精度和小数位数
        if (column.getRatio() >= 0 && column.getColumnSize() > 0) {
            if (typeName.contains("decimal") || typeName.contains("numeric")) {
                ddl.append("(").append(column.getColumnSize()).append(",")
                        .append(column.getRatio()).append(")");
            }
        }

        // 处理可空性
        if (Boolean.FALSE.equals(column.getNullable())) {
            ddl.append(" NOT NULL");
            // SQL Server 语法要求：向非空表添加 NOT NULL 列时，必须提供 DEFAULT 值
            // 注意：这是为了满足 SQL Server 的语法约束，不是通用的缺省值处理
            // 生成的 DEFAULT 值仅用于满足语法要求，不会影响数据同步结果
            // column.getTypeName() 已经是 SQL Server 类型名称（从系统表查询出来的）
            String defaultValue = SqlServerTemplate.getDefaultValueForNotNullColumnByTypeName(column.getTypeName());
            if (defaultValue != null) {
                ddl.append(" DEFAULT ").append(defaultValue);
            }
        }

        return ddl.toString();
    }

    private String generateDropColumnDDL(String tableName, Field column) {
        // 生成标准格式的 DDL（不带方括号），与 CDC 方式保持一致，便于 JSQLParser 解析
        if (schema != null && !schema.trim().isEmpty()) {
            return String.format("ALTER TABLE %s.%s DROP COLUMN %s",
                    schema, tableName, column.getName());
        } else {
            return String.format("ALTER TABLE %s DROP COLUMN %s",
                    tableName, column.getName());
        }
    }

    /**
     * 生成 RENAME COLUMN 的 DDL
     * 使用标准的 CHANGE COLUMN 语法（类似 MySQL），便于 JSQLParser 解析
     * DDL 解析器会自动转换为目标数据库的语法：
     * - 同构 SQL Server：IRToSQLServerConverter 会将其转换为 sp_rename
     * - 异构场景：可以转换为目标数据库的 RENAME 语法
     * <p>
     * 格式：ALTER TABLE table CHANGE COLUMN old_name new_name type [NOT NULL]
     */
    private String generateRenameColumnDDL(String tableName, Field oldColumn, Field newColumn) {
        StringBuilder ddl = new StringBuilder();

        // 构建表名（带 schema）
        if (schema != null && !schema.trim().isEmpty()) {
            ddl.append("ALTER TABLE ").append(schema).append(".").append(tableName).append(" ");
        } else {
            ddl.append("ALTER TABLE ").append(tableName).append(" ");
        }

        // CHANGE COLUMN old_name new_name
        ddl.append("CHANGE COLUMN ").append(oldColumn.getName())
                .append(" ").append(newColumn.getName()).append(" ");

        // 添加类型信息
        ddl.append(newColumn.getTypeName());

        // 处理长度/精度
        if (newColumn.getColumnSize() > 0) {
            String typeName = newColumn.getTypeName().toLowerCase();
            if (typeName.contains("varchar") || typeName.contains("char") || typeName.contains("nvarchar") || typeName.contains("nchar")) {
                ddl.append("(").append(newColumn.getColumnSize()).append(")");
            }
        }

        // 处理数值类型的精度和小数位数
        if (newColumn.getRatio() >= 0 && newColumn.getColumnSize() > 0) {
            String typeName = newColumn.getTypeName().toLowerCase();
            if (typeName.contains("decimal") || typeName.contains("numeric")) {
                ddl.append("(").append(newColumn.getColumnSize()).append(",")
                        .append(newColumn.getRatio()).append(")");
            }
        }

        // 处理可空性
        if (Boolean.FALSE.equals(newColumn.getNullable())) {
            ddl.append(" NOT NULL");
        }

        return ddl.toString();
    }

    private String generateAlterColumnDDL(String tableName, Field newCol) {
        StringBuilder ddl = new StringBuilder();
        // 生成标准格式的 DDL（不带方括号），与 CDC 方式保持一致，便于 JSQLParser 解析
        if (schema != null && !schema.trim().isEmpty()) {
            ddl.append("ALTER TABLE ").append(schema).append(".").append(tableName).append(" ");
        } else {
            ddl.append("ALTER TABLE ").append(tableName).append(" ");
        }
        ddl.append("ALTER COLUMN ").append(newCol.getName()).append(" ");
        ddl.append(newCol.getTypeName());

        // 处理长度/精度
        String typeName = newCol.getTypeName().toLowerCase();
        if (newCol.getColumnSize() > 0) {
            if (typeName.contains("varchar") || typeName.contains("char") || typeName.contains("varbinary") || typeName.contains("binary")) {
                ddl.append("(").append(newCol.getColumnSize()).append(")");
            }
        } else if (newCol.getColumnSize() == -1) {
            // SQL Server 中 -1 表示 MAX 长度（VARCHAR(MAX), NVARCHAR(MAX), VARBINARY(MAX)）
            if (typeName.contains("varchar") || typeName.contains("varbinary")) {
                ddl.append("(MAX)");
            }
        }

        // 处理数值类型的精度和小数位数
        if (newCol.getRatio() >= 0 && newCol.getColumnSize() > 0) {
            if (typeName.contains("decimal") || typeName.contains("numeric")) {
                ddl.append("(").append(newCol.getColumnSize()).append(",")
                        .append(newCol.getRatio()).append(")");
            }
        }

        // 处理可空性
        if (Boolean.FALSE.equals(newCol.getNullable())) {
            ddl.append(" NOT NULL");
        } else {
            ddl.append(" NULL");
        }

        return ddl.toString();
    }

    private String generateAlterPrimaryKeyDDL(String tableName, List<String> oldPrimaryKeys, List<String> newPrimaryKeys) {
        // 主键变更需要先删除旧主键，再添加新主键
        // 这是一个复杂操作，暂时返回警告信息
        logger.warn("主键变更暂不支持: table={}, oldPK={}, newPK={}",
                tableName, oldPrimaryKeys, newPrimaryKeys);
        return String.format("-- 主键变更暂不支持: %s", tableName);
    }

    /**
     * 从 JSON 字符串直接保存表结构快照（避免解析 JSON 的开销）
     *
     * @param tableName        表名
     * @param schemaInfoJson   表结构信息的 JSON 字符串（来自 INFORMATION_SCHEMA.COLUMNS）
     * @param version          版本号
     * @param hash             哈希值
     * @param ordinalPositions 列位置信息
     */
    private void saveTableSchemaSnapshotFromJson(String tableName, String schemaInfoJson, Long version,
                                                 String hash, Map<String, Integer> ordinalPositions) throws Exception {
        // 完全不持久化任何 schema 相关信息到 snapshot，只更新内存缓存
        MetaInfo metaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
        schemaCache.put(tableName, metaInfo);
        ordinalPositionsCache.put(tableName, ordinalPositions);
        schemaHashCache.put(tableName, hash);
        // 注意：不需要缓存版本号，DDL 变更检测时使用 getCurrentVersion() 获取当前的 Change Tracking 版本号
        // 注意：不再调用 super.refreshEvent(null)，因为不需要持久化 schema 信息
        // Change Tracking 版本号的持久化由其他逻辑处理（如 VERSION_POSITION）
    }

    private Map<String, Integer> getColumnOrdinalPositions(String tableName) throws Exception {
        // 只从内存缓存加载
        Map<String, Integer> cachedOrdinalPositions = ordinalPositionsCache.get(tableName);
        return cachedOrdinalPositions != null ? cachedOrdinalPositions : new HashMap<>();
    }

    /**
     * 启动时初始化所有表的 schema 缓存
     * 预先加载所有表的 schema 信息到内存缓存中，确保重启后首次检测时就能进行 DDL 变更检测
     */
    private void initializeSchemaCache() throws Exception {
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }

        logger.info("开始初始化 schema 缓存，共 {} 个表", tables.size());

        // 优化：在循环外调用一次 getCurrentVersion()，所有表共享同一个版本号
        Long currentVersion = getCurrentVersion();

        for (String tableName : tables) {
            String querySql = sqlTemplate.buildSchemeOnly(schema, tableName);

            String schemaInfoJson = queryUtil.queryAndMap(querySql, rs -> {
                String value = rs.getString("schema_info");
                return rs.wasNull() ? null : value;
            });

            if (schemaInfoJson == null || schemaInfoJson.trim().isEmpty()) {
                String msg = String.format("表 %s 的表结构信息为空，跳过初始化", tableName);
                logger.warn(msg);
                throw new RuntimeException(msg);
            }
            // 计算哈希值
            String hash = DigestUtils.md5Hex(schemaInfoJson);
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);

            // 更新内存缓存
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, hash, ordinalPositions);

            // 同时更新主键和列数缓存（如果缓存为空）
            MetaInfo metaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
            List<String> primaryKeys = getPrimaryKeysFromMetaInfo(metaInfo);
            int columnCount = ordinalPositions.size();
            primaryKeysCache.put(tableName, primaryKeys);
            tableColumnCountCache.put(tableName, columnCount);

            logger.debug("表 {} 的 schema 缓存初始化成功", tableName);
        }
    }

    protected void sleepInMills(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    // ==================== Worker 线程 ====================

    final class Worker extends Thread {
        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    // 直接获取最新版本号
                    Long maxVersion = getMaxVersion();
                    if (maxVersion != null && maxVersion > lastVersion) {
                        pull(lastVersion, maxVersion);
                    } else {
                        // 没有新版本，等待一段时间后继续轮询
                        sleepInMills(POLL_INTERVAL_MILLIS);
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    if (connected) {
                        logger.error("轮询版本号失败: {}", e.getMessage(), e);
                        sleepInMills(1000L);
                    }
                }
            }
        }
    }
}


