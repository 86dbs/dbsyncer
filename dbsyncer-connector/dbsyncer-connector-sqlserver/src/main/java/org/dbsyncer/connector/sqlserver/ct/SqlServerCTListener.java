package org.dbsyncer.connector.sqlserver.ct;

import org.apache.commons.codec.digest.DigestUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.ct.model.*;
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

import java.sql.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * SQL Server Change Tracking (CT) 监听器实现
 * 使用 Change Tracking 替代 CDC
 *
 * @Author Auto-generated
 * @Version 1.0.0
 */
public class SqlServerCTListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // Snapshot 键名
    private static final String VERSION_POSITION = "version";
    private static final String SCHEMA_SNAPSHOT_PREFIX = "schema_snapshot_";
    private static final String SCHEMA_HASH_PREFIX = "schema_hash_";
    private static final String SCHEMA_VERSION_PREFIX = "schema_version_";
    private static final String SCHEMA_TIME_PREFIX = "schema_time_";
    private static final String SCHEMA_ORDINAL_PREFIX = "schema_ordinal_";

    private final SqlServerTemplate sqlTemplate;

    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Set<String> tables;
    private DatabaseConnectorInstance instance;
    private Worker worker;
    private Long lastVersion;
    private String serverName;
    private String schema;
    private String realDatabaseName;
    private final int BUFFER_CAPACITY = 256;
    private BlockingQueue<Long> buffer = new LinkedBlockingQueue<>(BUFFER_CAPACITY);
    private final BlockingQueue<CTDDLEvent> ddlEventQueue = new LinkedBlockingQueue<>();
    private Lock lock = new ReentrantLock(true);
    private Condition isFull = lock.newCondition();
    private final Duration pollInterval = Duration.of(500, ChronoUnit.MILLIS);
    // 主键信息缓存（表名 -> 主键列表）
    private final Map<String, List<String>> primaryKeysCache = new HashMap<>();
    // 表列数缓存（表名 -> 列数），避免重复查询 INFORMATION_SCHEMA
    private final Map<String, Integer> tableColumnCountCache = new HashMap<>();

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

            worker = new Worker();
            worker.setName(new StringBuilder("ct-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
            worker.setDaemon(false);
            worker.start();
            VersionPuller.addExtractor(metaId, this);
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
            VersionPuller.removeExtractor(metaId);
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
        return queryAndMap(sqlTemplate.buildGetCurrentVersionSql(), rs -> {
            Long version = rs.getLong(1);
            return rs.wasNull() ? null : version;
        });
    }

    public Long getLastVersion() {
        return lastVersion;
    }

    public void pushStopVersion(Long version) {
        if (buffer.contains(version)) {
            return;
        }
        if (!buffer.offer(version)) {
            try {
                lock.lock();
                while (!buffer.offer(version) && connected) {
                    logger.warn("[{}] 缓存队列容量已达上限[{}], 正在阻塞重试.", this.getClass().getSimpleName(), BUFFER_CAPACITY);
                    try {
                        this.isFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
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
        tables = queryAndMapList(sql, rs -> {
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
        realDatabaseName = queryAndMap(sqlTemplate.buildGetDatabaseNameSql(), rs -> rs.getString(1));
        Integer count = queryAndMap(sqlTemplate.buildIsDatabaseChangeTrackingEnabledSql(realDatabaseName), rs -> rs.getInt(1));
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
                Integer count = query(checkSql, null, rs -> {
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

    private void pull(Long stopVersion) throws Exception {
        // 1. 批量查询所有表的主键信息和列数（合并查询，减少 IO 开销）
        Map<String, List<String>> tablePrimaryKeys = new HashMap<>();
        Map<String, Integer> tableColumnCounts = new HashMap<>();
        for (String table : tables) {
            List<String> primaryKeys = primaryKeysCache.get(table);
            Integer columnCount = tableColumnCountCache.get(table);

            // 如果主键或列数未缓存，使用合并查询一次性获取
            if (primaryKeys == null || columnCount == null) {
                TableMetaInfo metaInfo = getPrimaryKeysAndColumnCount(table);
                if (metaInfo.primaryKeys.isEmpty()) {
                    throw new SqlServerException("表 " + table + " 没有主键，无法使用 Change Tracking");
                }
                if (primaryKeys == null) {
                    primaryKeysCache.put(table, metaInfo.primaryKeys);
                }
                if (columnCount == null) {
                    tableColumnCountCache.put(table, metaInfo.columnCount);
                }
                primaryKeys = metaInfo.primaryKeys;
                columnCount = metaInfo.columnCount;
            }
            tablePrimaryKeys.put(table, primaryKeys);
            tableColumnCounts.put(table, columnCount);
        }

        // 2. 查询 DML 变更（传入主键信息和列数，避免重复查询）
        List<CTEvent> dmlEvents = new ArrayList<>();
        for (String table : tables) {
            List<CTEvent> tableEvents = pullDMLChanges(table, lastVersion, stopVersion,
                    tablePrimaryKeys.get(table));
            dmlEvents.addAll(tableEvents);
        }

        // 2. 查询 DDL 变更
        List<CTDDLEvent> ddlEvents = pullDDLChanges(lastVersion, stopVersion);

        // 3. 合并并按版本号排序
        List<CTUnifiedChangeEvent> unifiedEvents = mergeAndSortEvents(ddlEvents, dmlEvents);

        // 4. 按顺序解析和发送
        parseUnifiedEvents(unifiedEvents, stopVersion);
    }

    private List<CTEvent> pullDMLChanges(String tableName, Long startVersion, Long stopVersion,
                                         List<String> primaryKeys) throws Exception {
        // 1. 使用调用者传入的主键信息（已在 pull 方法中批量查询并缓存）
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new SqlServerException("表 " + tableName + " 没有主键，无法使用 Change Tracking");
        }

        // 2. 构建表结构信息子查询（用于附加到结果集中）
        String schemaInfoSubquery = sqlTemplate.buildGetTableSchemaInfoSubquery(schema, tableName);

        // 3. 构建主查询 SQL（包含表结构信息的 JSON 字段）
        String mainQuery = sqlTemplate.buildChangeTrackingDMLMainQuery(schema, tableName, primaryKeys, schemaInfoSubquery);

        // 4. 构建辅助查询 SQL（当没有 DML 变更时，用于获取 DDL 信息）
        String fallbackQuery = sqlTemplate.buildChangeTrackingDMLFallbackQuery(schema, tableName, schemaInfoSubquery);

        // 7. 先执行主查询，获取 DML 变更和 DDL 信息
        List<CTEvent> events = queryAndMapList(mainQuery, statement -> {
            statement.setLong(1, startVersion);   // CHANGETABLE 起始版本
            statement.setLong(2, startVersion);   // WHERE 起始版本
            statement.setLong(3, stopVersion);     // WHERE 结束版本
        }, rs -> {
            ResultSetMetaData metaData = rs.getMetaData();

            // 查找表结构信息列的位置（特殊列，不用于数据同步）
            int schemaInfoColumnIndex = -1;
            int resultColumnCount = metaData.getColumnCount();
            for (int i = 1; i <= resultColumnCount; i++) {
                if (SqlServerTemplate.CT_DDL_SCHEMA_INFO_COLUMN.equalsIgnoreCase(metaData.getColumnName(i))) {
                    schemaInfoColumnIndex = i;
                    break;
                }
            }

            // 在第一次获取结果时检测表结构（DDL 变更）
            // 使用 JSON 字段中的表结构信息进行 DDL 检测
            boolean hasData = false;
            try {
                if (schemaInfoColumnIndex > 0 && rs.next()) {
                    hasData = true;
                    // 获取表结构信息的 JSON（从第一行获取，因为所有行的表结构信息都相同）
                    String schemaInfoJson = rs.getString(schemaInfoColumnIndex);
                    if (schemaInfoJson != null && !schemaInfoJson.trim().isEmpty()) {
                        detectDDLChangesFromSchemaInfoJson(tableName, schemaInfoJson);
                    }
                }
            } catch (Exception e) {
                logger.error("检测 DDL 变更失败: {}", e.getMessage(), e);
            }

            // CT 主键列在固定位置（4 到 4+PK-1），建立主键列名到 CT 主键列索引的映射
            // 列顺序：SYS_CHANGE_VERSION(1), SYS_CHANGE_OPERATION(2), SYS_CHANGE_COLUMNS(3), 
            //        __CT_PK_[pk1](4), __CT_PK_[pk2](5), ..., __CT_PK_[pkN](4+PK-1),
            //        T.* 的所有列(4+PK 到 4+PK+N-1),
            //        __DDL_SCHEMA_INFO__(最后)
            Map<String, Integer> primaryKeyToCTIndex = new HashMap<>();
            int ctPkStartIndex = 4;  // CT 主键列从第 4 列开始
            for (int i = 0; i < primaryKeys.size(); i++) {
                String pk = primaryKeys.get(i);
                primaryKeyToCTIndex.put(pk, ctPkStartIndex + i);
            }

            // T.* 的列范围：从 4+PK 开始，到表结构信息列之前
            int tStarStartIndex = 4 + primaryKeys.size();  // T.* 从 CT 主键列之后开始
            int tStarEndIndex = schemaInfoColumnIndex > 0 ? schemaInfoColumnIndex - 1 : resultColumnCount - 1;

            // 需要跳过的列：表结构信息列
            Set<Integer> columnsToSkip = new HashSet<>();
            if (schemaInfoColumnIndex > 0) {
                columnsToSkip.add(schemaInfoColumnIndex);
            }

            // 性能优化：预先构建列名列表和列索引映射（列名对所有行都相同，避免重复调用 getColumnName）
            List<String> columnNames = new ArrayList<>();
            Map<Integer, String> columnIndexToName = new HashMap<>();
            Set<String> primaryKeySet = new HashSet<>(primaryKeys);  // 使用 Set 提高查找效率
            for (int i = tStarStartIndex; i <= tStarEndIndex; i++) {
                if (columnsToSkip.contains(i)) {
                    continue;
                }
                String columnName = metaData.getColumnName(i);
                columnNames.add(columnName);
                columnIndexToName.put(i, columnName);
            }

            List<CTEvent> dmlEvents = new ArrayList<>();

            // 如果第一行已经读取（用于 DDL 检测），需要处理这一行的 DML 数据
            if (hasData) {
                CTEvent event = processRow(rs, tableName, tStarStartIndex, tStarEndIndex, columnsToSkip,
                        columnIndexToName, columnNames, primaryKeySet, primaryKeyToCTIndex);
                if (event != null) {
                    dmlEvents.add(event);
                }
            }

            // 继续处理后续行
            while (rs.next()) {
                CTEvent event = processRow(rs, tableName, tStarStartIndex, tStarEndIndex, columnsToSkip,
                        columnIndexToName, columnNames, primaryKeySet, primaryKeyToCTIndex);
                if (event != null) {
                    dmlEvents.add(event);
                }
            }
            return dmlEvents;
        });

        // 8. 如果主查询没有返回数据，执行辅助查询获取 DDL 信息
        if (events.isEmpty()) {
            queryAndMapList(fallbackQuery, statement -> {
                statement.setLong(1, startVersion);   // EXISTS 子查询的 CHANGETABLE 起始版本
                statement.setLong(2, startVersion);   // EXISTS 子查询的 WHERE 起始版本
                statement.setLong(3, stopVersion);     // EXISTS 子查询的 WHERE 结束版本
            }, rs -> {
                if (rs.next()) {
                    // 获取表结构信息的 JSON
                    String schemaInfoJson = rs.getString(1);
                    if (schemaInfoJson != null && !schemaInfoJson.trim().isEmpty()) {
                        try {
                            detectDDLChangesFromSchemaInfoJson(tableName, schemaInfoJson);
                        } catch (Exception e) {
                            logger.error("检测 DDL 变更失败: {}", e.getMessage(), e);
                        }
                    }
                }
                return null;
            });
        }

        return events;
    }

    private List<CTDDLEvent> pullDDLChanges(Long startVersion, Long stopVersion) throws Exception {
        List<CTDDLEvent> events = new ArrayList<>();

        // 从队列中取出所有在版本范围内的 DDL 事件
        CTDDLEvent ddlEvent;
        while ((ddlEvent = ddlEventQueue.poll()) != null) {
            if (ddlEvent.getVersion() > startVersion && ddlEvent.getVersion() <= stopVersion) {
                // 检查表名是否匹配
                if (filterTable.contains(ddlEvent.getTableName())) {
                    events.add(ddlEvent);
                }
            } else if (ddlEvent.getVersion() > stopVersion) {
                // 版本号超过范围，放回队列
                ddlEventQueue.offer(ddlEvent);
                break;
            }
            // 版本号小于等于 startVersion 的事件直接丢弃（已处理过）
        }

        // 按版本号排序
        events.sort(Comparator.comparing(CTDDLEvent::getVersion));

        return events;
    }

    private List<CTUnifiedChangeEvent> mergeAndSortEvents(
            List<CTDDLEvent> ddlEvents,
            List<CTEvent> dmlEvents) {
        List<CTUnifiedChangeEvent> unifiedEvents = new ArrayList<>();

        // 添加 DDL 事件
        for (CTDDLEvent ddlEvent : ddlEvents) {
            unifiedEvents.add(new CTUnifiedChangeEvent(
                    "DDL",
                    ddlEvent.getTableName(),
                    ddlEvent.getDdlCommand(),
                    null,
                    ddlEvent.getVersion()
            ));
        }

        // 添加 DML 事件
        for (CTEvent dmlEvent : dmlEvents) {
            unifiedEvents.add(new CTUnifiedChangeEvent(
                    "DML",
                    dmlEvent.getTableName(),
                    null,
                    dmlEvent,
                    dmlEvent.getVersion()
            ));
        }

        // 按版本号排序
        unifiedEvents.sort(CTUnifiedChangeEvent.versionComparator());

        return unifiedEvents;
    }

    private void parseUnifiedEvents(List<CTUnifiedChangeEvent> events, Long stopVersion) {
        for (int i = 0; i < events.size(); i++) {
            boolean isEnd = i == events.size() - 1;
            CTUnifiedChangeEvent unifiedEvent = events.get(i);

            if ("DDL".equals(unifiedEvent.getEventType())) {
                // 发送 DDL 事件
                DDLChangedEvent ddlEvent = new DDLChangedEvent(
                        unifiedEvent.getTableName(),
                        ConnectorConstant.OPERTION_ALTER,
                        unifiedEvent.getDdlCommand(),
                        null,
                        isEnd ? stopVersion : null  // ChangedOffset 支持 Long 类型
                );
                sendChangedEvent(ddlEvent);
            } else {
                // 发送 DML 事件
                CTEvent ctevent = unifiedEvent.getCtevent();
                if (ctevent != null) {
                    RowChangedEvent rowEvent = new RowChangedEvent(
                            ctevent.getTableName(),
                            ctevent.getCode(),
                            ctevent.getRow(),
                            null,
                            isEnd ? stopVersion : null,
                            ctevent.getColumnNames()  // 使用CTEvent中保存的列名信息
                    );
                    sendChangedEvent(rowEvent);
                }
            }
        }

        // 统一更新版本号（DDL 和 DML 共享同一个版本号）
        lastVersion = stopVersion;
        snapshot.put(VERSION_POSITION, String.valueOf(lastVersion));
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
        return queryAndMap(sqlTemplate.buildGetCurrentVersionSql(), rs -> {
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
        return queryWithReadUncommitted(sql, statement -> {
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
                    columnCount = queryAndMapList(
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
     * 从 JSON 字段中的表结构信息检测 DDL 变更（优先使用，更准确）
     *
     * @param tableName      表名
     * @param schemaInfoJson 表结构信息的 JSON 字符串（来自 INFORMATION_SCHEMA.COLUMNS）
     */
    private void detectDDLChangesFromSchemaInfoJson(String tableName, String schemaInfoJson) throws Exception {
        if (schemaInfoJson == null || schemaInfoJson.trim().isEmpty()) {
            logger.warn("表 {} 的表结构信息 JSON 为空，跳过 DDL 检测", tableName);
            return;
        }

        // 1. 直接基于 JSON 字符串计算哈希值（快速，避免解析 JSON 的开销）
        String currentHash = DigestUtils.md5Hex(schemaInfoJson);
        String lastHash = getSchemaHash(tableName);

        if (lastHash == null) {
            // 首次检测，直接保存 JSON 字符串（避免解析 JSON 的开销）
            Long currentVersion = getCurrentVersion();
            Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
            saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);

            // 首次检测时也更新缓存（如果缓存为空）
            if (primaryKeysCache.get(tableName) == null || tableColumnCountCache.get(tableName) == null) {
                MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
                List<String> currentPrimaryKeys = getPrimaryKeysFromMetaInfo(currentMetaInfo);
                int currentColumnCount = ordinalPositions.size();
                primaryKeysCache.put(tableName, currentPrimaryKeys);
                tableColumnCountCache.put(tableName, currentColumnCount);
                logger.debug("首次检测表 {} 时更新缓存：主键={}, 列数={}", tableName, currentPrimaryKeys, currentColumnCount);
            }
            return;
        }

        if (lastHash.equals(currentHash)) {
            return; // 哈希值未变化，无 DDL 变更（避免解析 JSON 的开销）
        }

        // 2. 哈希值变化，解析 JSON 构建 MetaInfo 进行详细比对
        MetaInfo currentMetaInfo = parseSchemaInfoJson(tableName, schemaInfoJson);
        Long currentVersion = getCurrentVersion();
        MetaInfo lastMetaInfo = loadTableSchemaSnapshot(tableName);

        if (lastMetaInfo != null) {
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

            // 6. 如果有变更，生成 DDL 事件
            if (!changes.isEmpty()) {
                for (DDLChange change : changes) {
                    CTDDLEvent ddlEvent = new CTDDLEvent(
                            tableName,
                            change.getDdlCommand(),
                            currentVersion,  // 使用当前 Change Tracking 版本号
                            new Date()
                    );

                    // 将 DDL 事件加入待处理队列（与 DML 事件合并处理）
                    ddlEventQueue.offer(ddlEvent);
                }

                // 4. 更新表结构快照（直接保存 JSON，避免解析）
                Map<String, Integer> ordinalPositions = extractOrdinalPositions(schemaInfoJson);
                saveTableSchemaSnapshotFromJson(tableName, schemaInfoJson, currentVersion, currentHash, ordinalPositions);

                // 5. 更新缓存：DDL 变更可能影响主键或列数，需要同步更新缓存
                // 检查是否有影响主键或列数的变更
                boolean needUpdateCache = false;
                for (DDLChange change : changes) {
                    DDLChangeType changeType = change.getChangeType();
                    // ADD_COLUMN 和 DROP_COLUMN 会影响列数
                    // ALTER_PRIMARY_KEY 会影响主键
                    if (changeType == DDLChangeType.ADD_COLUMN ||
                            changeType == DDLChangeType.DROP_COLUMN ||
                            changeType == DDLChangeType.ALTER_PRIMARY_KEY) {
                        needUpdateCache = true;
                        break;
                    }
                }

                if (needUpdateCache) {
                    // 使用当前的表结构信息更新缓存
                    primaryKeysCache.put(tableName, currentPrimaryKeys);
                    // 列数可以从 schemaInfoJson 中提取（列数 = JSON 数组的长度）
                    int currentColumnCount = extractOrdinalPositions(schemaInfoJson).size();
                    tableColumnCountCache.put(tableName, currentColumnCount);
                    logger.debug("已更新表 {} 的缓存：主键={}, 列数={}", tableName, currentPrimaryKeys, currentColumnCount);
                }

                logger.info("检测到表 {} 的 DDL 变更，共 {} 个变更", tableName, changes.size());
            } else {
                // 即使没有检测到变更，也更新哈希值（可能是精度/尺寸变化导致哈希变化但无法检测）
                snapshot.put(SCHEMA_HASH_PREFIX + tableName, currentHash);
            }
        }
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
        // 直接保存 JSON 字符串（不需要解析）
        String schemaKey = SCHEMA_SNAPSHOT_PREFIX + tableName;
        snapshot.put(schemaKey, schemaInfoJson);

        // 保存哈希值
        String hashKey = SCHEMA_HASH_PREFIX + tableName;
        snapshot.put(hashKey, hash);

        // 保存版本号
        String versionKey = SCHEMA_VERSION_PREFIX + tableName;
        snapshot.put(versionKey, String.valueOf(version));

        // 保存时间戳
        String timeKey = SCHEMA_TIME_PREFIX + tableName;
        snapshot.put(timeKey, String.valueOf(System.currentTimeMillis()));

        // 保存列位置信息
        if (ordinalPositions != null && !ordinalPositions.isEmpty()) {
            String ordinalKey = SCHEMA_ORDINAL_PREFIX + tableName;
            String ordinalJson = JsonUtil.objToJson(ordinalPositions);
            snapshot.put(ordinalKey, ordinalJson);
        }

        super.refreshEvent(null);
    }

    private MetaInfo loadTableSchemaSnapshot(String tableName) throws Exception {
        String key = SCHEMA_SNAPSHOT_PREFIX + tableName;
        String json = snapshot.get(key);
        if (json == null || json.isEmpty()) {
            return null;
        }

        // 直接解析 INFORMATION_SCHEMA 格式的 JSON 为 MetaInfo
        return parseSchemaInfoJson(tableName, json);
    }

    private String getSchemaHash(String tableName) {
        String key = SCHEMA_HASH_PREFIX + tableName;
        return snapshot.get(key);
    }

    private Map<String, Integer> getColumnOrdinalPositions(String tableName) throws Exception {
        String key = SCHEMA_ORDINAL_PREFIX + tableName;
        String json = snapshot.get(key);
        if (json == null || json.isEmpty()) {
            return new HashMap<>();
        }
        return JsonUtil.jsonToObj(json, Map.class);
    }


    // ==================== 查询工具方法 ====================

    private interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    private interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    private <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) throws Exception {
        return queryAndMap(sql, null, mapper);
    }

    private <T> T queryAndMap(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) throws Exception {
        return query(sql, statementPreparer, (rs) -> {
            rs.next();
            return mapper.apply(rs);
        });
    }

    private <T> T queryAndMapList(String sql, ResultSetMapper<T> mapper) throws Exception {
        return queryAndMapList(sql, null, mapper);
    }

    private <T> T queryAndMapList(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) throws Exception {
        return query(sql, statementPreparer, mapper);
    }

    private <T> T query(String preparedQuerySql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) throws Exception {
        // 输出 SQL 语句用于调试
        logger.debug("执行查询 SQL: {}", preparedQuerySql);
        Object execute = instance.execute(databaseTemplate -> {
            PreparedStatement ps = null;
            ResultSet rs = null;
            T apply = null;
            try {
                ps = databaseTemplate.getSimpleConnection().prepareStatement(preparedQuerySql);
                if (null != statementPreparer) {
                    statementPreparer.accept(ps);
                }
                rs = ps.executeQuery();
                apply = mapper.apply(rs);
            } catch (Exception e) {
                logger.error("查询失败，SQL: {}, 错误: {}", preparedQuerySql, e.getMessage(), e);
                throw e;
            } finally {
                close(rs);
                close(ps);
            }
            return apply;
        });
        return (T) execute;
    }

    /**
     * 使用 READ UNCOMMITTED 隔离级别查询表元信息，避免死锁
     * 当执行 DDL 操作时，查询 INFORMATION_SCHEMA 可能会与 DDL 操作产生死锁
     * 使用 READ UNCOMMITTED 隔离级别可以避免等待锁，减少死锁概率
     */
    private <T> T queryWithReadUncommitted(String preparedQuerySql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) throws Exception {
        final int maxRetries = 3;
        final long retryDelayMs = 100;
        Exception lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Object execute = instance.execute(databaseTemplate -> {
                    Connection conn = databaseTemplate.getSimpleConnection();
                    int originalIsolation = conn.getTransactionIsolation();
                    PreparedStatement ps = null;
                    ResultSet rs = null;
                    T apply = null;
                    try {
                        // 设置 READ UNCOMMITTED 隔离级别，避免等待锁
                        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                        ps = conn.prepareStatement(preparedQuerySql);
                        if (null != statementPreparer) {
                            statementPreparer.accept(ps);
                        }
                        rs = ps.executeQuery();
                        apply = mapper.apply(rs);
                    } finally {
                        // 恢复原始隔离级别
                        try {
                            conn.setTransactionIsolation(originalIsolation);
                        } catch (SQLException e) {
                            logger.warn("恢复事务隔离级别失败: {}", e.getMessage());
                        }
                        close(rs);
                        close(ps);
                    }
                    return apply;
                });
                return (T) execute;
            } catch (SQLException e) {
                // 检查是否是死锁错误（错误代码 1205）
                if (e.getErrorCode() == 1205 || e.getMessage().contains("死锁") || e.getMessage().contains("deadlock")) {
                    lastException = e;
                    if (attempt < maxRetries - 1) {
                        logger.warn("查询时发生死锁，重试 {}/{}: {}", attempt + 1, maxRetries, e.getMessage());
                        try {
                            Thread.sleep(retryDelayMs * (attempt + 1)); // 递增延迟
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new SQLException("重试被中断", ie);
                        }
                        continue;
                    }
                }
                // 非死锁错误或重试次数已用完，直接抛出
                logger.error("查询失败: {}, sql: {}", e.getMessage(), preparedQuerySql, e);
                throw e;
            } catch (Exception e) {
                logger.error("查询失败: {}", e.getMessage(), e);
                throw e;
            }
        }

        // 所有重试都失败
        if (lastException != null) {
            throw lastException;
        }
        throw new SQLException("查询失败：未知错误");
    }

    private void close(AutoCloseable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
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
                    Long stopVersion = buffer.take();
                    Long poll;
                    while ((poll = buffer.poll()) != null) {
                        stopVersion = poll;
                    }
                    if (stopVersion == null || stopVersion <= lastVersion) {
                        continue;
                    }

                    pull(stopVersion);

                    // 版本号已在 parseUnifiedEvents() 中统一更新，这里不需要重复更新
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    if (connected) {
                        logger.error(e.getMessage(), e);
                        sleepInMills(1000L);
                    }
                }
            }
        }
    }
}


