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

    // SQL 常量
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "SELECT db_name()";
    private static final String GET_TABLE_LIST = "SELECT name FROM sys.tables WHERE schema_id = schema_id('#') AND is_ms_shipped = 0";

    // Change Tracking 相关 SQL
    private static final String GET_CURRENT_VERSION = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

    // 特殊列名，用于标识表结构信息的 JSON 字段（不用于数据同步）
    private static final String DDL_SCHEMA_INFO_COLUMN = "__DDL_SCHEMA_INFO__";

    // 获取表结构信息的子查询（用于在 DML 查询中附加表结构信息）
    // 注意：SQL Server 2008 R2 不支持 FOR JSON PATH，使用字符串拼接方式生成 JSON
    // 使用 STUFF + FOR XML PATH 来拼接 JSON 数组字符串
    private static final String GET_TABLE_SCHEMA_INFO_SUBQUERY =
            "(SELECT " +
                    "    '[' + STUFF((" +
                    "        SELECT ',' + " +
                    "            '{' + " +
                    "            '\"COLUMN_NAME\":\"' + REPLACE(COLUMN_NAME, '\"', '\\\"') + '\",' + " +
                    "            '\"DATA_TYPE\":\"' + REPLACE(DATA_TYPE, '\"', '\\\"') + '\",' + " +
                    "            CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL " +
                    "                THEN '\"CHARACTER_MAXIMUM_LENGTH\":' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ',' " +
                    "                ELSE '' END + " +
                    "            CASE WHEN NUMERIC_PRECISION IS NOT NULL " +
                    "                THEN '\"NUMERIC_PRECISION\":' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' " +
                    "                ELSE '' END + " +
                    "            CASE WHEN NUMERIC_SCALE IS NOT NULL " +
                    "                THEN '\"NUMERIC_SCALE\":' + CAST(NUMERIC_SCALE AS VARCHAR) + ',' " +
                    "                ELSE '' END + " +
                    "            '\"IS_NULLABLE\":\"' + IS_NULLABLE + '\",' + " +
                    "            '\"ORDINAL_POSITION\":' + CAST(ORDINAL_POSITION AS VARCHAR) + " +
                    "            '}' " +
                    "        FROM INFORMATION_SCHEMA.COLUMNS " +
                    "        WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' " +
                    "        ORDER BY ORDINAL_POSITION " +
                    "        FOR XML PATH(''), TYPE " +
                    "    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + ']' AS schema_info " +
                    " FROM (SELECT 1 AS dummy) AS t)";

    private static final String GET_DML_CHANGES =
            "SELECT " +
                    "    CT.SYS_CHANGE_VERSION, " +
                    "    CT.SYS_CHANGE_OPERATION, " +
                    "    CT.SYS_CHANGE_COLUMNS, " +
                    "    T.*, " +
                    "    %s AS " + DDL_SCHEMA_INFO_COLUMN + " " +
                    "FROM CHANGETABLE(CHANGES [%s].[%s], ?) AS CT " +
                    "LEFT JOIN [%s].[%s] AS T ON %s " +
                    "WHERE CT.SYS_CHANGE_VERSION > ? AND CT.SYS_CHANGE_VERSION <= ? " +
                    "ORDER BY CT.SYS_CHANGE_VERSION ASC";

    private static final String GET_TABLE_COLUMNS =
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                    "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION " +
                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                    "ORDER BY ORDINAL_POSITION";

    private static final String GET_TABLE_PRIMARY_KEYS =
            "SELECT kcu.COLUMN_NAME " +
                    "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                    "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " +
                    "    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
                    "    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA " +
                    "    AND tc.TABLE_NAME = kcu.TABLE_NAME " +
                    "WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? " +
                    "    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                    "ORDER BY kcu.ORDINAL_POSITION";

    // 启用 Change Tracking
    private static final String ENABLE_DB_CHANGE_TRACKING =
            "ALTER DATABASE [%s] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";

    private static final String ENABLE_TABLE_CHANGE_TRACKING =
            "ALTER TABLE [%s].[%s] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";

    private static final String IS_DB_CHANGE_TRACKING_ENABLED =
            "SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID('%s')";

    private static final String IS_TABLE_CHANGE_TRACKING_ENABLED =
            "SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('%s.%s')";

    // Snapshot 键名
    private static final String VERSION_POSITION = "version";
    private static final String SCHEMA_SNAPSHOT_PREFIX = "schema_snapshot_";
    private static final String SCHEMA_HASH_PREFIX = "schema_hash_";
    private static final String SCHEMA_VERSION_PREFIX = "schema_version_";
    private static final String SCHEMA_TIME_PREFIX = "schema_time_";
    private static final String SCHEMA_ORDINAL_PREFIX = "schema_ordinal_";

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
        }
    }

    public Long getMaxVersion() throws Exception {
        return queryAndMap(GET_CURRENT_VERSION, rs -> {
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
                super.forceFlushEvent();
                return;
            }
            throw new SqlServerException("No Change Tracking version available");
        }
        lastVersion = Long.valueOf(snapshot.get(VERSION_POSITION));
    }

    private void readTables() throws Exception {
        tables = queryAndMapList(GET_TABLE_LIST.replace(STATEMENTS_PLACEHOLDER, schema), rs -> {
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
        realDatabaseName = queryAndMap(GET_DATABASE_NAME, rs -> rs.getString(1));
        Integer count = queryAndMap(String.format(IS_DB_CHANGE_TRACKING_ENABLED, realDatabaseName), rs -> rs.getInt(1));
        if (count == null || count == 0) {
            execute(String.format(ENABLE_DB_CHANGE_TRACKING, realDatabaseName));
            logger.info("已启用数据库 [{}] 的 Change Tracking", realDatabaseName);
        }
    }

    private void enableTableChangeTracking() {
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        tables.forEach(table -> {
            try {
                String checkSql = String.format(IS_TABLE_CHANGE_TRACKING_ENABLED, schema, table);
                Integer count = query(checkSql, null, rs -> {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                    return 0;
                });
                if (count == null || count == 0) {
                    try {
                        execute(String.format(ENABLE_TABLE_CHANGE_TRACKING, schema, table));
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
        // 1. 查询 DML 变更
        List<CTEvent> dmlEvents = new ArrayList<>();
        for (String table : tables) {
            List<CTEvent> tableEvents = pullDMLChanges(table, lastVersion, stopVersion);
            dmlEvents.addAll(tableEvents);
        }

        // 2. 查询 DDL 变更
        List<CTDDLEvent> ddlEvents = pullDDLChanges(lastVersion, stopVersion);

        // 3. 合并并按版本号排序
        List<CTUnifiedChangeEvent> unifiedEvents = mergeAndSortEvents(ddlEvents, dmlEvents);

        // 4. 按顺序解析和发送
        parseUnifiedEvents(unifiedEvents, stopVersion);
    }

    private List<CTEvent> pullDMLChanges(String tableName, Long startVersion, Long stopVersion) throws Exception {
        // 1. 获取表的主键列
        List<String> primaryKeys = getPrimaryKeys(tableName);
        if (primaryKeys.isEmpty()) {
            throw new SqlServerException("表 " + tableName + " 没有主键，无法使用 Change Tracking");
        }

        // 2. 构建 JOIN 条件（支持复合主键）
        String joinCondition = primaryKeys.stream()
                .map(pk -> "CT.[" + pk + "] = T.[" + pk + "]")
                .collect(Collectors.joining(" AND "));

        // 3. 构建表结构信息子查询（用于附加到结果集中）
        String schemaInfoSubquery = String.format(GET_TABLE_SCHEMA_INFO_SUBQUERY, schema, tableName);

        // 4. 构建查询 SQL（包含表结构信息的 JSON 字段）
        String sql = String.format(GET_DML_CHANGES,
                schemaInfoSubquery,  // 表结构信息子查询
                schema, tableName,  // CHANGETABLE
                schema, tableName,  // JOIN table
                joinCondition       // JOIN condition（支持复合主键）
        );

        // 5. 查询变更
        return queryAndMapList(sql, statement -> {
            statement.setLong(1, startVersion);   // CHANGETABLE 起始版本
            statement.setLong(2, startVersion);   // WHERE 起始版本
            statement.setLong(3, stopVersion);     // WHERE 结束版本
        }, rs -> {
            ResultSetMetaData metaData = rs.getMetaData();

            // 查找表结构信息列的位置（特殊列，不用于数据同步）
            int schemaInfoColumnIndex = -1;
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                if (DDL_SCHEMA_INFO_COLUMN.equalsIgnoreCase(metaData.getColumnName(i))) {
                    schemaInfoColumnIndex = i;
                    break;
                }
            }

            // 在第一次获取结果时检测表结构（DDL 变更）
            // 使用 JSON 字段中的表结构信息进行 DDL 检测
            boolean firstRowProcessed = false;
            try {
                if (schemaInfoColumnIndex > 0 && rs.next()) {
                    // 获取表结构信息的 JSON（从第一行获取，因为所有行的表结构信息都相同）
                    String schemaInfoJson = rs.getString(schemaInfoColumnIndex);
                    if (schemaInfoJson != null && !schemaInfoJson.trim().isEmpty()) {
                        detectDDLChangesFromSchemaInfoJson(tableName, schemaInfoJson);
                    }
                    firstRowProcessed = true;
                }
            } catch (Exception e) {
                logger.error("检测 DDL 变更失败: {}", e.getMessage(), e);
            }

            List<CTEvent> events = new ArrayList<>();
            
            // 如果第一行已经读取（用于 DDL 检测），需要处理这一行的数据
            if (firstRowProcessed) {
                Long version = rs.getLong(1);
                String operation = rs.getString(2);  // 'I', 'U', 'D'
                // 跳过 SYS_CHANGE_COLUMNS（第 3 列），不使用

                // 构建行数据（排除表结构信息列）
                List<Object> row = new ArrayList<>();
                for (int i = 4; i <= columnCount; i++) {
                    // 跳过表结构信息列（不用于数据同步）
                    if (i == schemaInfoColumnIndex) {
                        continue;
                    }
                    row.add(rs.getObject(i));
                }

                // 转换操作类型
                String operationCode = convertOperation(operation);

                events.add(new CTEvent(tableName, operationCode, row, version));
            }
            
            // 继续处理后续行
            while (rs.next()) {
                Long version = rs.getLong(1);
                String operation = rs.getString(2);  // 'I', 'U', 'D'
                // 跳过 SYS_CHANGE_COLUMNS（第 3 列），不使用

                // 构建行数据（排除表结构信息列）
                List<Object> row = new ArrayList<>();
                for (int i = 4; i <= columnCount; i++) {
                    // 跳过表结构信息列（不用于数据同步）
                    if (i == schemaInfoColumnIndex) {
                        continue;
                    }
                    row.add(rs.getObject(i));
                }

                // 转换操作类型
                String operationCode = convertOperation(operation);

                events.add(new CTEvent(tableName, operationCode, row, version));
            }
            return events;
        });
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
                            null  // 列名信息（可选）
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
        return queryAndMap(GET_CURRENT_VERSION, rs -> {
            Long version = rs.getLong(1);
            return rs.wasNull() ? null : version;
        });
    }

    private List<String> getPrimaryKeys(String tableName) throws Exception {
        // 使用 READ UNCOMMITTED 隔离级别避免死锁
        return queryWithReadUncommitted(GET_TABLE_PRIMARY_KEYS, statement -> {
            statement.setString(1, schema);
            statement.setString(2, tableName);
        }, rs -> {
            List<String> pks = new ArrayList<>();
            while (rs.next()) {
                pks.add(rs.getString("COLUMN_NAME"));
            }
            return pks;
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
            List<DDLChange> changes = compareTableSchema(tableName, lastMetaInfo, currentMetaInfo,
                    lastPrimaryKeys, currentPrimaryKeys,
                    lastOrdinalPositions, currentOrdinalPositions);

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
        List<String> primaryKeys = getPrimaryKeys(tableName);

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
            Boolean nullable = "YES".equalsIgnoreCase(isNullableStr);

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

    private String calculateSchemaHashFromMetaData(ResultSetMetaData metaData) throws SQLException {
        StringBuilder hashInput = new StringBuilder();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            // 跳过 Change Tracking 系统列（SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION, SYS_CHANGE_COLUMNS）
            if (i <= 3) continue;
            hashInput.append(metaData.getColumnName(i))
                    .append("|").append(metaData.getColumnTypeName(i))
                    .append("|").append(metaData.getColumnDisplaySize(i))
                    .append("|").append(metaData.getPrecision(i))
                    .append("|").append(metaData.getScale(i))
                    .append("|").append(metaData.isNullable(i))
                    .append("|");
        }
        return DigestUtils.md5Hex(hashInput.toString());
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
            if (oldCol != null && !isColumnEqual(oldCol, newCol)) {
                // 列属性变更
                String ddl = generateAlterColumnDDL(tableName, oldCol, newCol);
                changes.add(new DDLChange(DDLChangeType.ALTER_COLUMN, ddl, newCol.getName()));
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

    private String generateAlterColumnDDL(String tableName, Field oldCol, Field newCol) {
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
     * @param tableName      表名
     * @param schemaInfoJson 表结构信息的 JSON 字符串（来自 INFORMATION_SCHEMA.COLUMNS）
     * @param version        版本号
     * @param hash           哈希值
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
        
        super.forceFlushEvent();
    }

    private void saveTableSchemaSnapshot(String tableName, MetaInfo metaInfo, Long version,
                                         String hash, Map<String, Integer> ordinalPositions) throws Exception {
        // 保存 MetaInfo（表结构）- 将 MetaInfo 转换为 JSON
        String schemaKey = SCHEMA_SNAPSHOT_PREFIX + tableName;
        String schemaJson = JsonUtil.objToJson(metaInfo);
        snapshot.put(schemaKey, schemaJson);

        // 保存哈希值
        String hashKey = SCHEMA_HASH_PREFIX + tableName;
        snapshot.put(hashKey, hash);

        // 保存版本号
        String versionKey = SCHEMA_VERSION_PREFIX + tableName;
        snapshot.put(versionKey, String.valueOf(version));

        // 保存时间戳
        String timeKey = SCHEMA_TIME_PREFIX + tableName;
        snapshot.put(timeKey, String.valueOf(System.currentTimeMillis()));

        // 保存列位置映射（序列化为 JSON）
        if (ordinalPositions != null && !ordinalPositions.isEmpty()) {
            String ordinalKey = SCHEMA_ORDINAL_PREFIX + tableName;
            String ordinalJson = JsonUtil.objToJson(ordinalPositions);
            snapshot.put(ordinalKey, ordinalJson);
        }

        super.forceFlushEvent();
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

    private MetaInfo queryTableMetaInfo(String tableName, Map<String, Integer> ordinalPositions) throws Exception {
        // 查询列信息（使用 READ UNCOMMITTED 隔离级别避免死锁）
        List<Field> columns = queryWithReadUncommitted(GET_TABLE_COLUMNS, statement -> {
            statement.setString(1, schema);
            statement.setString(2, tableName);
        }, rs -> {
            List<Field> cols = new ArrayList<>();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("DATA_TYPE");
                Integer maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null
                        ? rs.getInt("CHARACTER_MAXIMUM_LENGTH") : null;
                Integer precision = rs.getObject("NUMERIC_PRECISION") != null
                        ? rs.getInt("NUMERIC_PRECISION") : null;
                Integer scale = rs.getObject("NUMERIC_SCALE") != null
                        ? rs.getInt("NUMERIC_SCALE") : null;
                Boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                Integer ordinalPosition = rs.getInt("ORDINAL_POSITION");

                // 创建 Field 对象
                Field col = new Field();
                col.setName(columnName);
                col.setTypeName(dataType);
                col.setColumnSize(maxLength != null ? maxLength : (precision != null ? precision : 0));
                col.setRatio(scale != null ? scale : 0);
                col.setNullable(nullable);

                // 保存列位置
                ordinalPositions.put(columnName, ordinalPosition);

                cols.add(col);
            }
            return cols;
        });

        // 查询主键（用于设置 Field.isPk()，使用 READ UNCOMMITTED 隔离级别避免死锁）
        List<String> primaryKeys = getPrimaryKeys(tableName);

        // 设置主键标识
        for (Field col : columns) {
            col.setPk(primaryKeys.contains(col.getName()));
        }

        // 创建 MetaInfo 对象
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setTableType("TABLE");
        metaInfo.setColumn(columns);

        return metaInfo;
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
                logger.error("查询失败: {}", e.getMessage(), e);
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
                logger.error("查询失败: {}", e.getMessage(), e);
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

