/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.cdc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.enums.TableOperationEnum;
import org.dbsyncer.connector.sqlserver.model.DMLEvent;
import org.dbsyncer.connector.sqlserver.model.DDLEvent;
import org.dbsyncer.connector.sqlserver.model.SqlServerChangeTable;
import org.dbsyncer.connector.sqlserver.model.UnifiedChangeEvent;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public class SqlServerListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_DATABASE_NAME = "select db_name()";
    private static final String GET_TABLE_LIST = "select name from sys.tables where schema_id = schema_id('#') and is_ms_shipped = 0";
    private static final String IS_DB_CDC_ENABLED = "select is_cdc_enabled from sys.databases where name = '#'";
    private static final String IS_TABLE_CDC_ENABLED = "select count(*) from sys.tables tb where tb.is_tracked_by_cdc = 1 and tb.name='#'";
    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name = '#' and is_cdc_enabled=0) EXEC sys.sp_cdc_enable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' and is_tracked_by_cdc=0) EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String DISABLE_TABLE_CDC = "EXEC sys.sp_cdc_disable_table @source_schema = N'%s', @source_name = N'#', @capture_instance = 'all'";
    private static final String GET_TABLE_COLUMNS = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'%s' AND TABLE_NAME = N'#' ORDER BY ORDINAL_POSITION";
    private static final String GET_TABLE_COLUMNS_WITH_TYPE = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'%s' AND TABLE_NAME = N'#' ORDER BY ORDINAL_POSITION";
    private static final String GET_CHANGE_TABLE_COLUMNS_WITH_TYPE = "SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, c.max_length AS CHARACTER_MAXIMUM_LENGTH, c.precision AS NUMERIC_PRECISION, c.scale AS NUMERIC_SCALE, c.is_nullable AS IS_NULLABLE, dc.definition AS COLUMN_DEFAULT FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id WHERE c.object_id = %d AND c.name NOT LIKE '__$%%' ORDER BY c.column_id";
    private static final String GET_TABLES_CDC_ENABLED = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String GET_MAX_LSN = "select sys.fn_cdc_get_max_lsn()";
    private static final String GET_MIN_LSN = "select sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_INCREMENT_LSN = "select sys.fn_cdc_increment_lsn(?)";
    /**
     * https://learn.microsoft.com/zh-cn/previous-versions/sql/sql-server-2008/bb510627(v=sql.100)?redirectedfrom=MSDN
     */
    private static final String GET_ALL_CHANGES_FOR_TABLE = "select * from cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC";

    // DDL 相关 SQL
    private static final String GET_DDL_CHANGES =
            "SELECT source_object_id, object_id, ddl_command, ddl_lsn, ddl_time " +
                    "FROM cdc.ddl_history " +
                    "WHERE ddl_lsn > ? AND ddl_lsn <= ? " +
                    "ORDER BY ddl_lsn ASC";

    // 获取 DDL 的最大 LSN
    private static final String GET_MAX_DDL_LSN =
            "SELECT MAX(ddl_lsn) FROM cdc.ddl_history";

    private static final String LSN_POSITION = "position";
    private static final int OFFSET_COLUMNS = 4;
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private Set<String> tables;
    private Set<SqlServerChangeTable> changeTables;
    private DatabaseConnectorInstance instance;
    private Worker worker;
    private Lsn lastLsn;
    private String serverName;
    private String schema;
    private final int BUFFER_CAPACITY = 256;
    private BlockingQueue<Lsn> buffer = new LinkedBlockingQueue<>(BUFFER_CAPACITY);
    private Lock lock = new ReentrantLock(true);
    private Condition isFull = lock.newCondition();
    private final Duration pollInterval = Duration.of(500, ChronoUnit.MILLIS);

    // 表名缓存：使用 Caffeine 本地缓存，10分钟过期
    private final Cache<Integer, String> objectIdToTableNameCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .maximumSize(1000)
            .build();

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("SqlServerExtractor is already started");
                return;
            }
            connected = true;
            connect();
            readTables();
            Assert.notEmpty(tables, "No tables available");

            enableDBCDC();
            enableTableCDC();
            readChangeTables();
            readLastLsn();

            worker = new Worker();
            worker.setName(new StringBuilder("cdc-parser-").append(serverName).append("_").append(worker.hashCode()).toString());
            worker.setDaemon(false);
            worker.start();
            LsnPuller.addExtractor(metaId, this);
        } catch (Exception e) {
            close();
            logger.error("启动失败:{}", e.getMessage());
            throw new SqlServerException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        if (connected) {
            LsnPuller.removeExtractor(metaId);
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
            snapshot.put(LSN_POSITION, offset.getPosition().toString());
        }
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

    private void connect() {
        instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;
        if (service.isAlive(instance)) {
            DatabaseConfig cfg = instance.getConfig();
            serverName = cfg.getUrl();
            schema = cfg.getSchema();
        }
    }

    private void readLastLsn() {
        if (!snapshot.containsKey(LSN_POSITION)) {
            lastLsn = queryAndMap(GET_MAX_LSN, rs -> new Lsn(rs.getBytes(1)));
            if (null != lastLsn && lastLsn.isAvailable()) {
                snapshot.put(LSN_POSITION, lastLsn.toString());
                super.forceFlushEvent();
                return;
            }
            // Shouldn't happen if the agent is running, but it is better to guard against such situation
            throw new SqlServerException("No maximum LSN recorded in the database");
        }
        lastLsn = Lsn.valueOf(snapshot.get(LSN_POSITION));
    }

    private void readTables() {
        tables = queryAndMapList(GET_TABLE_LIST.replace(STATEMENTS_PLACEHOLDER, schema), rs -> {
            Set<String> table = new LinkedHashSet<>();
            while (rs.next()) {
                if (filterTable.contains(rs.getString(1))) {
                    table.add(rs.getString(1));
                }
            }
            return table;
        });
    }

    private void readChangeTables() {
        changeTables = queryAndMapList(GET_TABLES_CDC_ENABLED, rs -> {
            final Set<SqlServerChangeTable> tables = new HashSet<>();
            while (rs.next()) {
                String schemaName = rs.getString(1);
                String tableName = rs.getString(2);

                // 只处理当前 schema 且属于 filterTable 的表
                // 注意：sys.sp_cdc_help_change_data_capture 返回数据库中所有启用 CDC 的表
                // 我们需要过滤，只保留当前 Listener 监控的表
                if (schema.equals(schemaName) && filterTable.contains(tableName)) {
                    SqlServerChangeTable changeTable = new SqlServerChangeTable(
                            // schemaName
                            schemaName,
                            // tableName
                            tableName,
                            // captureInstance
                            rs.getString(3),
                            // changeTableObjectId
                            rs.getInt(4),
                            // startLsn
                            rs.getBytes(6),
                            // stopLsn
                            rs.getBytes(7),
                            // capturedColumns
                            rs.getString(15));
                    tables.add(changeTable);
                }
            }
            return tables;
        });
    }

    private void enableTableCDC() {
        if (!CollectionUtils.isEmpty(tables)) {
            tables.forEach(table -> {
                boolean enabledTableCDC = queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs -> rs.getInt(1) > 0);
                if (!enabledTableCDC) {
                    execute(String.format(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table), schema));
                    Lsn minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs -> new Lsn(rs.getBytes(1)));
                    logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
                }
            });
        }
    }

    private void enableDBCDC() throws InterruptedException {
        String realDatabaseName = queryAndMap(GET_DATABASE_NAME, rs -> rs.getString(1));
        boolean enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
        if (!enabledCDC) {
            execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, realDatabaseName));
            // make sure it works
            TimeUnit.SECONDS.sleep(3);

            enabledCDC = queryAndMap(IS_DB_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, realDatabaseName), rs -> rs.getBoolean(1));
            Assert.isTrue(enabledCDC, "Please ensure that the SQL Server Agent is running");
        }
    }

    private void execute(String... sqlStatements) {
        instance.execute(databaseTemplate -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    logger.info("executing '{}'", sqlStatement);
                    databaseTemplate.execute(sqlStatement);
                }
            }
            return true;
        });
    }

    private void pull(Lsn stopLsn) {
        Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));

        // 查询 DML 变更（提取 LSN 用于排序）
        // 注意：在每次 rs.next() 之前都检查列名是否变化（用于检测 sp_rename 重命名列的情况）
        // 因为 sp_rename 可能发生在多个 DML 操作之间，所以需要在处理每一行数据之前都检查
        // 如果检测到列名变化，先发送 DDL 事件并重新启用 CDC，然后刷新 changeTable 信息
        List<DMLEvent> dmlEvents = new ArrayList<>();
        changeTables.forEach(changeTable -> {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            // 使用数组来保存 changeTable 引用，以便在 lambda 中更新
            final SqlServerChangeTable[] currentChangeTableRef = new SqlServerChangeTable[]{changeTable};

            List<DMLEvent> list = queryAndMapList(query, statement -> {
                statement.setBytes(1, startLsn.getBinary());
                statement.setBytes(2, stopLsn.getBinary());
            }, rs -> {
                int columnCount = rs.getMetaData().getColumnCount();
                List<Object> row = null;
                List<DMLEvent> data = new ArrayList<>();

                while (rs.next()) {
                    // 在每次 rs.next() 之前，检查列名是否变化
                    // 如果检测到 sp_rename，先发送 DDL 事件并重新启用 CDC，然后更新 changeTable 信息
                    // 注意：checkAndReEnableCDCIfNeeded 会直接更新传入的 changeTable 对象，无需重新查找
                    checkAndReEnableCDCIfNeeded(currentChangeTableRef[0]);

                    final int operation = rs.getInt(3);  // __$operation 是第 3 列
                    if (TableOperationEnum.isUpdateBefore(operation)) {
                        continue;
                    }

                    // 提取 LSN（第 1 列是 __$start_lsn）
                    Lsn eventLsn = new Lsn(rs.getBytes(1));

                    row = new ArrayList<>(columnCount - OFFSET_COLUMNS);
                    for (int i = OFFSET_COLUMNS + 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }

                    // 创建 CDCEvent，包含 LSN 信息和列名信息
                    // 注意：直接使用 changeTable 的列列表（如果检测到 sp_rename 并重新启用了 CDC，这里会自动使用新列名）
                    DMLEvent DMLEvent = new DMLEvent(currentChangeTableRef[0].getTableName(), operation, row, eventLsn, 
                            new ArrayList<>(currentChangeTableRef[0].getCapturedColumnList()));
                    data.add(DMLEvent);
                }
                return data;
            });

            if (!CollectionUtils.isEmpty(list)) {
                dmlEvents.addAll(list);
            }
        });

        // 查询 DDL 变更（使用统一的 LSN）
        List<DDLEvent> ddlEvents = pullDDLChanges(startLsn, stopLsn);

        // 合并并按 LSN 排序
        List<UnifiedChangeEvent> unifiedEvents = mergeAndSortEvents(ddlEvents, dmlEvents);

        // 按顺序解析和发送（统一更新 LSN）
        parseUnifiedEvents(unifiedEvents, stopLsn);
    }

    public void trySendEvent(RowChangedEvent event) {
        while (connected) {
            try {
                sendChangedEvent(event);
                break;
            } catch (QueueOverflowException ex) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException exe) {
                    logger.error(exe.getMessage(), exe);
                }
            }
        }
    }

    /**
     * 查询 DDL 变更（使用统一的 LSN）
     */
    private List<DDLEvent> pullDDLChanges(Lsn startLsn, Lsn stopLsn) {
        return queryAndMapList(GET_DDL_CHANGES, statement -> {
            statement.setBytes(1, startLsn.getBinary());
            statement.setBytes(2, stopLsn.getBinary());
        }, rs -> {
            List<DDLEvent> events = new ArrayList<>();
            while (rs.next()) {
                String ddlCommand = rs.getString(3);
                Lsn ddlLsn = new Lsn(rs.getBytes(4));
                java.util.Date ddlTime = rs.getTimestamp(5);

                String tableName = extractTableNameFromDDL(ddlCommand);
                if (tableName == null) {
                    logger.warn("无法从 DDL 命令中解析表名或 schema: {}", ddlCommand);
                    continue;
                }

                logger.info("接收到 DDL : {}", ddlCommand);

                // 过滤 DDL 类型：只处理 ALTER TABLE
                if (!isTableAlterDDL(ddlCommand)) {
                    continue;
                }

                // 检查 schema 和表名是否匹配
                if (filterTable.contains(tableName)) {
                    events.add(new DDLEvent(tableName, ddlCommand, ddlLsn, ddlTime));
                }
            }
            return events;
        });
    }

    /**
     * 判断是否为 ALTER TABLE 类型的 DDL
     */
    private boolean isTableAlterDDL(String ddlCommand) {
        if (ddlCommand == null) {
            return false;
        }
        return ddlCommand.toUpperCase().trim().startsWith("ALTER TABLE");
    }

    /**
     * 合并并排序 DDL 和 DML 事件
     */
    private List<UnifiedChangeEvent> mergeAndSortEvents(
            List<DDLEvent> ddlEvents,
            List<DMLEvent> dmlEvents) {

        List<UnifiedChangeEvent> unifiedEvents = new ArrayList<>();

        // 转换 DDL 事件
        ddlEvents.forEach(ddlEvent -> {
            UnifiedChangeEvent event = new UnifiedChangeEvent();
            event.setLsn(ddlEvent.getDdlLsn());
            event.setEventType("DDL");
            event.setTableName(ddlEvent.getTableName());
            event.setDdlCommand(ddlEvent.getDdlCommand());
            unifiedEvents.add(event);
        });

        // 转换 DML 事件
        dmlEvents.forEach(dmlEvent -> {
            UnifiedChangeEvent event = new UnifiedChangeEvent();
            event.setLsn(dmlEvent.getLsn());
            event.setEventType("DML");
            event.setTableName(dmlEvent.getTableName());
            event.setCdcEvent(dmlEvent);
            unifiedEvents.add(event);
        });

        // 按 LSN 排序
        unifiedEvents.sort((e1, e2) -> {
            Lsn lsn1 = e1.getLsn();
            Lsn lsn2 = e2.getLsn();
            if (lsn1 == null && lsn2 == null) return 0;
            if (lsn1 == null) return -1;
            if (lsn2 == null) return 1;
            return lsn1.compareTo(lsn2);
        });

        return unifiedEvents;
    }

    /**
     * 解析并发送统一事件（统一更新 LSN）
     */
    private void parseUnifiedEvents(List<UnifiedChangeEvent> events, Lsn stopLsn) {
        for (int i = 0; i < events.size(); i++) {
            boolean isEnd = i == events.size() - 1;
            UnifiedChangeEvent unifiedEvent = events.get(i);

            if ("DDL".equals(unifiedEvent.getEventType())) {
                trySendDDLEvent(
                        unifiedEvent.getTableName(),
                        unifiedEvent.getDdlCommand(),
                        isEnd ? stopLsn : null
                );
            } else {
                // 发送 DML 事件
                DMLEvent DMLEvent = unifiedEvent.getCdcEvent();
                // 获取列名信息（从 DMLEvent 中获取）
                List<String> columnNames = DMLEvent != null ? DMLEvent.getColumnNames() : null;
                if (TableOperationEnum.isUpdateAfter(DMLEvent.getCode())) {
                    trySendEvent(new RowChangedEvent(
                            DMLEvent.getTableName(),
                            ConnectorConstant.OPERTION_UPDATE,
                            DMLEvent.getRow(),
                            null,
                            (isEnd ? stopLsn : null),
                            columnNames
                    ));
                } else if (TableOperationEnum.isInsert(DMLEvent.getCode())) {
                    trySendEvent(new RowChangedEvent(
                            DMLEvent.getTableName(),
                            ConnectorConstant.OPERTION_INSERT,
                            DMLEvent.getRow(),
                            null,
                            (isEnd ? stopLsn : null),
                            columnNames
                    ));
                } else if (TableOperationEnum.isDelete(DMLEvent.getCode())) {
                    trySendEvent(new RowChangedEvent(
                            DMLEvent.getTableName(),
                            ConnectorConstant.OPERTION_DELETE,
                            DMLEvent.getRow(),
                            null,
                            (isEnd ? stopLsn : null),
                            columnNames
                    ));
                }
            }
        }

        // 统一更新 LSN（DDL 和 DML 共享同一个 LSN）
        lastLsn = stopLsn;
        snapshot.put(LSN_POSITION, lastLsn.toString());
    }

    /**
     * 发送 DDL 事件
     */
    private void trySendDDLEvent(String tableName, String ddlCommand, Lsn position) {

        // 使用 isFilterTable 方法进行匹配（它会检查 schema 和表名）
        DDLChangedEvent ddlEvent = new DDLChangedEvent(
                tableName,
                ConnectorConstant.OPERTION_ALTER,
                ddlCommand,
                null,
                position
        );

        while (connected) {
            try {
                sendChangedEvent(ddlEvent);

                // 如果 DDL 操作会影响列结构（增删改列），需要重新启用表的 CDC
                if (isColumnStructureChangedDDL(ddlCommand)) {
                    reEnableTableCDC(tableName);
                }

                break;
            } catch (QueueOverflowException ex) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException exe) {
                    logger.error(exe.getMessage(), exe);
                }
            }
        }
    }

    /**
     * 判断 DDL 操作是否会改变列结构（需要重新启用 CDC）
     * <p>
     * SQL Server CDC 的限制：当表已启用 CDC 后，列结构的变更不会自动反映到 CDC 捕获列表中。
     * 因此，任何会影响列结构的 DDL 操作都需要重新启用表的 CDC。
     * <p>
     * 支持的 DDL 操作类型：
     * - ALTER TABLE ... ADD ... - 添加列（SQL Server 语法：ADD column_name，不需要 COLUMN 关键字）
     * - ALTER TABLE ... DROP COLUMN ... - 删除列
     * - ALTER TABLE ... ALTER COLUMN ... - 修改列（类型、约束等）
     * <p>
     * 注意：SQL Server 使用 sp_rename 重命名列，但该操作不会出现在 cdc.ddl_history 中，
     * 因为 sp_rename 不是标准的 ALTER TABLE 语句。如果需要在重命名后更新 CDC，需要手动处理。
     */
    private boolean isColumnStructureChangedDDL(String ddlCommand) {
        if (ddlCommand == null) {
            return false;
        }
        String upperCommand = ddlCommand.toUpperCase().trim();

        // 必须是 ALTER TABLE 操作
        if (!upperCommand.startsWith("ALTER TABLE")) {
            return false;
        }

        // 检查是否包含会影响列结构的操作
        // SQL Server 语法：
        // - ADD column_name (不需要 COLUMN 关键字)
        // - DROP COLUMN column_name
        // - ALTER COLUMN column_name
        return upperCommand.contains(" ADD ") ||           // 添加列
                upperCommand.contains("DROP COLUMN") ||     // 删除列
                upperCommand.contains("ALTER COLUMN");     // 修改列
    }

    /**
     * 检查表的列名是否与 CDC 捕获的列名一致，如果不一致则重新启用 CDC
     * <p>
     * 用于检测 sp_rename 重命名列的情况（sp_rename 不会出现在 cdc.ddl_history 中）
     * <p>
     * 注意：此方法已改为 public，以便在测试中直接调用以触发 sp_rename 检测
     */
    public void checkAndReEnableCDCIfNeeded(SqlServerChangeTable changeTable) {
        try {
            String tableName = changeTable.getTableName();
            List<String> capturedColumnList = changeTable.getCapturedColumnList();

            if (CollectionUtils.isEmpty(capturedColumnList)) {
                return;
            }

            // 获取当前表的所有列名
            List<String> currentColumns = queryAndMapList(
                    String.format(GET_TABLE_COLUMNS, schema, tableName),
                    rs -> {
                        List<String> colList = new ArrayList<>();
                        while (rs.next()) {
                            colList.add(rs.getString(1));
                        }
                        return colList;
                    }
            );

            if (CollectionUtils.isEmpty(currentColumns)) {
                return;
            }

            // 快速路径：使用 hash 值快速判断列名是否变化
            // 计算当前列的 hash 值
            int currentColumnsHash = SqlServerChangeTable.calculateHash(currentColumns);
            if (currentColumnsHash == changeTable.getCapturedColumnsHash()) {
                // hash 值相同，说明列名没有变化，可以快速返回
                return;
            }

            // 比较列数和列名
            if (currentColumns.size() != capturedColumnList.size()) {
                // 列数不同，说明可能有列增删操作（但这种情况应该已经被 DDL 事件捕获）
                // 为了安全起见，重新启用 CDC
                logger.warn("表 [{}] 的列数与 CDC 捕获的列数不一致（当前: {}, CDC: {}），重新启用 CDC",
                        tableName, currentColumns.size(), capturedColumnList.size());
                reEnableTableCDC(tableName);
                // 直接更新 changeTable 的 capturedColumns，避免后续需要从 changeTables 中重新查找
                changeTable.updateCapturedColumns(currentColumns);
                return;
            }

            // 检查列名是否完全一致（用于检测 sp_rename 重命名列）
            Set<String> currentColumnSet = new HashSet<>(currentColumns);
            Set<String> capturedColumnSet = new HashSet<>(capturedColumnList);

            if (!currentColumnSet.equals(capturedColumnSet)) {
                // 列名不一致，说明可能有列重命名操作（sp_rename）
                logger.warn("表 [{}] 的列名与 CDC 捕获的列名不一致，检测到可能的列重命名操作（sp_rename）", tableName);
                logger.debug("当前列: {}, CDC 列: {}", currentColumns, capturedColumnList);

                // 使用基于 hash 的匹配算法检测并发送列重命名 DDL 事件
                detectAndSendRenameDDLEventsWithHash(tableName, changeTable, currentColumns, capturedColumnList);

                // 重新启用 CDC
                reEnableTableCDC(tableName);
                // 直接更新 changeTable 的 capturedColumns，避免后续需要从 changeTables 中重新查找
                changeTable.updateCapturedColumns(currentColumns);
            }

        } catch (Exception e) {
            logger.error("检查表 [{}] 的列名一致性时出错: {}", changeTable.getTableName(), e.getMessage(), e);
        }
    }

    /**
     * 使用基于表级别 hash 的匹配算法检测列重命名并发送 DDL 事件
     * <p>
     * 优化思路：
     * 1. 计算整个表的 hash（基于所有列的元数据，不包括列名）
     * 2. 如果表的 hash 相同但列名不同，说明是重命名操作
     * 3. 如果表的 hash 不同，说明是增删或类型变化
     * <p>
     * 优势：
     * - 性能更好：只需要计算一次表级别的 hash
     * - 逻辑更简单：不需要维护列级别的 hash 映射
     * - 更准确：表级别的 hash 相同意味着列结构（除列名外）完全一致
     */
    private void detectAndSendRenameDDLEventsWithHash(String tableName, SqlServerChangeTable changeTable,
                                                      List<String> currentColumns, List<String> capturedColumns) {
        try {
            // 1. 获取当前表的列详细信息（包括类型）
            // 注意：使用 sys.columns 查询以确保与 change table 的查询方式一致
            List<ColumnInfo> currentColumnInfos = queryAndMapList(
                    String.format("SELECT c.name AS COLUMN_NAME, t.name AS DATA_TYPE, c.max_length AS CHARACTER_MAXIMUM_LENGTH, c.precision AS NUMERIC_PRECISION, c.scale AS NUMERIC_SCALE, c.is_nullable AS IS_NULLABLE, dc.definition AS COLUMN_DEFAULT FROM sys.columns c INNER JOIN sys.tables tb ON c.object_id = tb.object_id INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id INNER JOIN sys.types t ON c.user_type_id = t.user_type_id LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id WHERE s.name = N'%s' AND tb.name = N'%s' ORDER BY c.column_id",
                            schema, tableName),
                    rs -> {
                        List<ColumnInfo> infos = new ArrayList<>();
                        while (rs.next()) {
                            ColumnInfo info = new ColumnInfo();
                            info.name = rs.getString("COLUMN_NAME");
                            info.dataType = rs.getString("DATA_TYPE");
                            Object maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH");
                            // SQL Server 的 max_length 对于 nvarchar 等类型需要除以 2
                            if (maxLength != null && (info.dataType.equals("nvarchar") || info.dataType.equals("nchar"))) {
                                info.maxLength = ((Number) maxLength).intValue() / 2;
                            } else {
                                info.maxLength = maxLength;
                            }
                            info.precision = rs.getObject("NUMERIC_PRECISION");
                            info.scale = rs.getObject("NUMERIC_SCALE");
                            info.nullable = rs.getBoolean("IS_NULLABLE");
                            info.defaultValue = rs.getString("COLUMN_DEFAULT");
                            infos.add(info);
                        }
                        return infos;
                    }
            );

            // 2. 获取 change table 的列详细信息（旧列的元数据）
            // 注意：change table 的列名是旧列名，但元数据应该与源表一致
            List<ColumnInfo> capturedColumnInfos = queryAndMapList(
                    String.format(GET_CHANGE_TABLE_COLUMNS_WITH_TYPE, changeTable.getChangeTableObjectId()),
                    rs -> {
                        List<ColumnInfo> infos = new ArrayList<>();
                        while (rs.next()) {
                            ColumnInfo info = new ColumnInfo();
                            info.name = rs.getString("COLUMN_NAME");
                            info.dataType = rs.getString("DATA_TYPE");
                            Object maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH");
                            // SQL Server 的 max_length 对于 nvarchar 等类型需要除以 2
                            if (maxLength != null && (info.dataType.equals("nvarchar") || info.dataType.equals("nchar"))) {
                                info.maxLength = ((Number) maxLength).intValue() / 2;
                            } else {
                                info.maxLength = maxLength;
                            }
                            info.precision = rs.getObject("NUMERIC_PRECISION");
                            info.scale = rs.getObject("NUMERIC_SCALE");
                            info.nullable = rs.getBoolean("IS_NULLABLE");
                            info.defaultValue = rs.getString("COLUMN_DEFAULT");
                            infos.add(info);
                        }
                        return infos;
                    }
            );

            // 3. 计算表级别的 hash（包含列名）
            // 优化思路：重命名是稀有事件，大部分情况下表结构没有变化
            // 如果包含列名的 hash 相同，说明完全没有变化，可以快速放行，提高吞吐率
            // 如果包含列名的 hash 不同，再进一步判断是重命名还是增删/类型变化
            String currentTableHashWithName = calculateTableHash(currentColumnInfos, true);
            String capturedTableHashWithName = calculateTableHash(capturedColumnInfos, true);

            logger.debug("表 [{}] 的 hash 比对（包含列名）: 当前表={}, 捕获表={}",
                    tableName, currentTableHashWithName, capturedTableHashWithName);

            // 4. 快速路径：如果包含列名的 hash 相同，说明完全没有变化，直接返回
            if (currentTableHashWithName.equals(capturedTableHashWithName)) {
                logger.debug("表 [{}] 的 hash（包含列名）相同，说明表结构完全没有变化，快速放行", tableName);
                return;
            }

            // 5. 慢速路径：hash 不同，需要进一步判断是重命名还是增删/类型变化
            // 计算不包含列名的 hash 来判断
            String currentTableHashWithoutName = calculateTableHash(currentColumnInfos, false);
            String capturedTableHashWithoutName = calculateTableHash(capturedColumnInfos, false);

            logger.debug("表 [{}] 的 hash 比对（不包含列名）: 当前表={}, 捕获表={}",
                    tableName, currentTableHashWithoutName, capturedTableHashWithoutName);

            if (currentTableHashWithoutName.equals(capturedTableHashWithoutName)) {
                // 包含列名的 hash 不同，但不包含列名的 hash 相同，说明是重命名
                logger.info("表 [{}] 检测到重命名：包含列名的 hash 不同，但不包含列名的 hash 相同", tableName);

                // 通过列名差异找出重命名的列
                List<RenamePair> renamePairs = findRenamePairsByTableHash(
                        currentColumnInfos, capturedColumnInfos, currentColumns, capturedColumns);

                // 6. 发送重命名 DDL 事件
                for (RenamePair pair : renamePairs) {
                    String ddlCommand = buildRenameColumnDDL(tableName, pair.oldName, pair.newName, pair.columnInfo);

                    logger.info("检测到列重命名（基于表 hash 匹配）: 表 [{}], 旧列名: [{}], 新列名: [{}], DDL: {}",
                            tableName, pair.oldName, pair.newName, ddlCommand);

                    // 发送 DDL 事件（使用当前 LSN，因为 sp_rename 没有 LSN）
                    trySendDDLEvent(tableName, ddlCommand, null);
                }

                if (renamePairs.isEmpty()) {
                    logger.info("表 [{}] 的 hash 匹配但未找到重命名列，可能是位置调整", tableName);
                }
            } else {
                // 两个 hash 都不同，说明是增删或类型变化，不是纯重命名
                logger.info("表 [{}] 的 hash（包含列名和不包含列名）都不同，判断为增删或类型变化，不发送重命名 DDL", tableName);
            }

        } catch (Exception e) {
            logger.error("使用表 hash 匹配检测列重命名时出错: {}", e.getMessage(), e);
            // 如果 hash 匹配失败，回退到原来的位置匹配算法
            logger.info("回退到位置匹配算法");
            detectAndSendRenameDDLEvents(tableName, currentColumns, capturedColumns);
        }
    }

    /**
     * 计算表级别的 hash（基于所有列的元数据）
     *
     * @param columnInfos       列信息列表
     * @param includeColumnName 是否包含列名
     *                          <p>
     *                          hash 包含的信息：
     *                          - 列的数量
     *                          - 每列的类型、长度、精度、是否可空、默认值（按列的顺序）
     *                          - 如果 includeColumnName 为 true，还包含列名
     */
    private String calculateTableHash(List<ColumnInfo> columnInfos, boolean includeColumnName) {
        // 按列的顺序构建 hash 字符串
        StringBuilder sb = new StringBuilder();
        sb.append(columnInfos.size()).append("|");

        for (ColumnInfo info : columnInfos) {
            // 如果包含列名，先添加列名
            if (includeColumnName) {
                sb.append(info.name != null ? info.name.toUpperCase() : "");
                sb.append("|");
            }

            // 添加列的元数据
            sb.append(info.dataType != null ? info.dataType.toUpperCase() : "");
            sb.append("|");
            sb.append(info.maxLength != null ? info.maxLength : "");
            sb.append("|");
            sb.append(info.precision != null ? info.precision : "");
            sb.append("|");
            sb.append(info.scale != null ? info.scale : "");
            sb.append("|");
            sb.append(info.nullable ? "NULL" : "NOTNULL");
            sb.append("|");
            sb.append(info.defaultValue != null ? info.defaultValue : "");
            sb.append("|");
        }

        // 使用简单的 hash（也可以使用 MD5 或 SHA-256）
        return String.valueOf(sb.toString().hashCode());
    }

    /**
     * 通过表 hash 匹配找出重命名的列对
     * <p>
     * 由于表的 hash 相同，说明列结构（除列名外）完全一致
     * 通过位置匹配找出列名不同的列对
     */
    private List<RenamePair> findRenamePairsByTableHash(List<ColumnInfo> currentColumnInfos,
                                                        List<ColumnInfo> capturedColumnInfos,
                                                        List<String> currentColumns,
                                                        List<String> capturedColumns) {
        List<RenamePair> renamePairs = new ArrayList<>();

        // 构建列名到列信息的映射
        Map<String, ColumnInfo> currentColumnInfoMap = new HashMap<>();
        for (ColumnInfo info : currentColumnInfos) {
            currentColumnInfoMap.put(info.name, info);
        }

        // 通过位置匹配找出重命名的列
        for (int i = 0; i < Math.min(currentColumns.size(), capturedColumns.size()); i++) {
            String currentCol = currentColumns.get(i);
            String capturedCol = capturedColumns.get(i);

            if (!currentCol.equals(capturedCol)) {
                // 检查是否为重命名（列名不同但位置相同）
                // 由于表 hash 相同，如果列名不同，更可能是重命名

                // 检查 capturedCol 是否在其他位置出现（如果出现，可能是位置变化，不是重命名）
                boolean capturedColExistsElsewhere = false;
                for (int j = 0; j < currentColumns.size(); j++) {
                    if (j != i && currentColumns.get(j).equals(capturedCol)) {
                        capturedColExistsElsewhere = true;
                        break;
                    }
                }

                // 检查 currentCol 是否在 capturedColumns 的其他位置出现
                boolean currentColExistsElsewhere = false;
                for (int j = 0; j < capturedColumns.size(); j++) {
                    if (j != i && capturedColumns.get(j).equals(currentCol)) {
                        currentColExistsElsewhere = true;
                        break;
                    }
                }

                // 如果两个列名都不在对方列表的其他位置出现，且表 hash 相同，则认为是重命名
                if (!capturedColExistsElsewhere && !currentColExistsElsewhere) {
                    ColumnInfo columnInfo = currentColumnInfoMap.get(currentCol);
                    if (columnInfo != null) {
                        renamePairs.add(new RenamePair(capturedCol, currentCol, columnInfo));
                    }
                }
            }
        }

        return renamePairs;
    }

    /**
     * 检测列重命名并发送 DDL 事件（用于触发目标端的列重命名）
     * <p>
     * 通过列的类型信息匹配来区分重命名和增删两种场景：
     * 1. 如果列数相同，且能找到类型匹配的列，则认为是重命名
     * 2. 如果列数不同，或找不到类型匹配的列，则认为是增删（不发送重命名 DDL）
     * <p>
     * 注意：这是回退算法，当 hash 匹配失败时使用
     */
    private void detectAndSendRenameDDLEvents(String tableName, List<String> currentColumns, List<String> capturedColumns) {
        try {
            // 获取当前表的列详细信息（包括类型）
            List<ColumnInfo> currentColumnInfos = queryAndMapList(
                    String.format(GET_TABLE_COLUMNS_WITH_TYPE, schema, tableName),
                    rs -> {
                        List<ColumnInfo> infos = new ArrayList<>();
                        while (rs.next()) {
                            ColumnInfo info = new ColumnInfo();
                            info.name = rs.getString("COLUMN_NAME");
                            info.dataType = rs.getString("DATA_TYPE");
                            info.maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH");
                            info.precision = rs.getObject("NUMERIC_PRECISION");
                            info.scale = rs.getObject("NUMERIC_SCALE");
                            info.nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                            info.defaultValue = rs.getString("COLUMN_DEFAULT");
                            infos.add(info);
                        }
                        return infos;
                    }
            );

            // 如果列数不同，更可能是增删操作，而不是重命名
            if (currentColumns.size() != capturedColumns.size()) {
                logger.info("表 [{}] 的列数发生变化（当前: {}, CDC: {}），判断为增删操作，不发送重命名 DDL",
                        tableName, currentColumns.size(), capturedColumns.size());
                return;
            }

            // 列数相同，尝试通过类型信息匹配找出重命名的列
            // 构建当前列的映射（列名 -> 列信息）
            Map<String, ColumnInfo> currentColumnInfoMap = new HashMap<>();
            for (ColumnInfo info : currentColumnInfos) {
                currentColumnInfoMap.put(info.name, info);
            }

            // 获取 CDC 捕获时的列信息（需要查询 change table 的元数据）
            // 注意：由于 CDC 已经重新启用，我们无法直接获取旧列的详细信息
            // 但我们可以通过位置和类型匹配来推断

            // 通过位置和类型匹配找出重命名的列
            List<RenamePair> renamePairs = new ArrayList<>();
            Set<String> matchedCurrentCols = new HashSet<>();
            Set<String> matchedCapturedCols = new HashSet<>();

            for (int i = 0; i < currentColumns.size(); i++) {
                String currentCol = currentColumns.get(i);
                String capturedCol = capturedColumns.get(i);

                if (currentCol.equals(capturedCol)) {
                    // 列名相同，跳过
                    matchedCurrentCols.add(currentCol);
                    matchedCapturedCols.add(capturedCol);
                    continue;
                }

                // 列名不同，尝试通过类型匹配
                ColumnInfo currentInfo = currentColumnInfoMap.get(currentCol);
                if (currentInfo == null) {
                    continue;
                }

                // 检查是否能在其他位置找到类型匹配的列（可能是重命名）
                // 由于我们无法获取旧列的详细信息，这里使用启发式方法：
                // 如果列数相同，且位置 i 的列名不同，但列的总数没有变化，可能是重命名
                // 更保守的策略：只处理列数相同且列名完全不同的情况

                // 检查 capturedCol 是否在其他位置出现（如果出现，可能是位置变化，不是重命名）
                boolean capturedColExistsElsewhere = false;
                for (int j = 0; j < currentColumns.size(); j++) {
                    if (j != i && currentColumns.get(j).equals(capturedCol)) {
                        capturedColExistsElsewhere = true;
                        break;
                    }
                }

                // 检查 currentCol 是否在 capturedColumns 的其他位置出现
                boolean currentColExistsElsewhere = false;
                for (int j = 0; j < capturedColumns.size(); j++) {
                    if (j != i && capturedColumns.get(j).equals(currentCol)) {
                        currentColExistsElsewhere = true;
                        break;
                    }
                }

                // 如果两个列名都在对方列表中出现，可能是位置变化，不是重命名
                if (capturedColExistsElsewhere || currentColExistsElsewhere) {
                    logger.debug("表 [{}] 位置 {} 的列名变化可能是位置调整，不是重命名: {} -> {}",
                            tableName, i, capturedCol, currentCol);
                    continue;
                }

                // 如果列名完全不同，且列数相同，更可能是重命名
                if (!matchedCurrentCols.contains(currentCol) && !matchedCapturedCols.contains(capturedCol)) {
                    renamePairs.add(new RenamePair(capturedCol, currentCol, currentInfo));
                    matchedCurrentCols.add(currentCol);
                    matchedCapturedCols.add(capturedCol);
                }
            }

            // 发送重命名 DDL 事件
            for (RenamePair pair : renamePairs) {
                String ddlCommand = buildRenameColumnDDL(tableName, pair.oldName, pair.newName, pair.columnInfo);

                logger.info("检测到列重命名: 表 [{}], 旧列名: [{}], 新列名: [{}], DDL: {}",
                        tableName, pair.oldName, pair.newName, ddlCommand);

                // 发送 DDL 事件（使用当前 LSN，因为 sp_rename 没有 LSN）
                trySendDDLEvent(tableName, ddlCommand, null);
            }

            if (renamePairs.isEmpty()) {
                logger.info("表 [{}] 的列名变化无法确定为重命名操作，可能是增删或位置调整", tableName);
            }

        } catch (Exception e) {
            logger.error("检测列重命名时出错: {}", e.getMessage(), e);
        }
    }

    /**
     * 重命名对（旧列名 -> 新列名）
     */
    private static class RenamePair {
        String oldName;
        String newName;
        ColumnInfo columnInfo;

        RenamePair(String oldName, String newName, ColumnInfo columnInfo) {
            this.oldName = oldName;
            this.newName = newName;
            this.columnInfo = columnInfo;
        }
    }

    /**
     * 构造列重命名的 DDL 语句
     * <p>
     * 使用 MySQL 的 CHANGE COLUMN 语法，因为 DDL 解析器能理解这个语法
     * DDL 解析器会自动转换为目标数据库的语法（如 SQL Server 的 sp_rename）
     */
    private String buildRenameColumnDDL(String tableName, String oldColumnName, String newColumnName, ColumnInfo columnInfo) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("ALTER TABLE ").append(tableName)
                .append(" CHANGE COLUMN ").append(oldColumnName)
                .append(" ").append(newColumnName);

        // 添加类型信息
        String typeStr = columnInfo.dataType;
        if (columnInfo.maxLength != null) {
            typeStr += "(" + columnInfo.maxLength + ")";
        } else if (columnInfo.precision != null) {
            typeStr += "(" + columnInfo.precision;
            if (columnInfo.scale != null) {
                typeStr += "," + columnInfo.scale;
            }
            typeStr += ")";
        }
        ddl.append(" ").append(typeStr);

        // 添加 NOT NULL 约束
        if (!columnInfo.nullable) {
            ddl.append(" NOT NULL");
        }

        // 添加 DEFAULT 值
        if (columnInfo.defaultValue != null && !columnInfo.defaultValue.trim().isEmpty()) {
            ddl.append(" DEFAULT ").append(columnInfo.defaultValue);
        }

        return ddl.toString();
    }

    /**
     * 列信息内部类
     */
    private static class ColumnInfo {
        String name;
        String dataType;
        Object maxLength;
        Object precision;
        Object scale;
        boolean nullable;
        String defaultValue;
    }

    /**
     * 重新启用表的 CDC（用于在列结构变更后更新捕获列列表）
     * <p>
     * 适用场景：
     * - ADD COLUMN: 需要捕获新列
     * - DROP COLUMN: 需要移除已删除的列
     * - RENAME COLUMN: 需要更新列名（sp_rename）
     * - ALTER COLUMN: 需要更新列类型/约束信息
     */
    private void reEnableTableCDC(String tableName) {
        try {
            // 1. 获取表的所有列名（包括新列）
            List<String> columns = queryAndMapList(
                    String.format(GET_TABLE_COLUMNS, schema, tableName),
                    rs -> {
                        List<String> colList = new ArrayList<>();
                        while (rs.next()) {
                            colList.add(rs.getString(1));
                        }
                        return colList;
                    }
            );

            if (CollectionUtils.isEmpty(columns)) {
                logger.warn("表 [{}] 没有列，跳过重新启用 CDC", tableName);
                return;
            }

            // 2. 禁用表的 CDC（所有 capture_instance）
            execute(String.format(DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, tableName), schema));
            logger.info("已禁用表 [{}] 的 CDC", tableName);

            // 等待一下，确保禁用操作完成
            TimeUnit.MILLISECONDS.sleep(500);

            // 3. 重新启用表的 CDC，指定所有列（包括新列）
            String columnList = String.join(", ", columns);
            String enableSql = String.format(
                    "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @supports_net_changes = 0, @captured_column_list = N'%s'",
                    schema, tableName, columnList
            );
            execute(enableSql);
            logger.info("已重新启用表 [{}] 的 CDC，捕获列: {}", tableName, columnList);

            // 4. 刷新 changeTables 缓存
            readChangeTables();
            logger.info("已刷新 changeTables 缓存");

        } catch (Exception e) {
            logger.error("重新启用表 [{}] 的 CDC 失败: {}", tableName, e.getMessage(), e);
        }
    }

    /**
     * 从 DDL 语句中提取表名（可能包含 schema）
     *
     * @return 完整表名，格式为 "schema.tableName" 或 "tableName"（如果没有 schema）
     */
    private String extractTableNameFromDDL(String ddlCommand) {
        try {
            Statement statement = CCJSqlParserUtil.parse(ddlCommand);
            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                String tableName = alter.getTable().getName();
                String schemaName = alter.getTable().getSchemaName();

                // 清理引号
                tableName = StringUtil.replace(tableName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                schemaName = StringUtil.replace(schemaName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);

                if (schema.equals(schemaName)) return tableName;
            }
        } catch (JSQLParserException e) {
            logger.warn("无法解析 DDL 语句: {}", ddlCommand);
        }
        return null;
    }

    private interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    private interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    private <T> T queryAndMap(String sql, ResultSetMapper<T> mapper) {
        return queryAndMap(sql, null, mapper);
    }

    private <T> T queryAndMap(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        return query(sql, statementPreparer, (rs) -> {
            rs.next();
            return mapper.apply(rs);
        });
    }

    private <T> T queryAndMapList(String sql, ResultSetMapper<T> mapper) {
        return queryAndMapList(sql, null, mapper);
    }

    private <T> T queryAndMapList(String sql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
        return query(sql, statementPreparer, mapper);
    }

    private <T> T query(String preparedQuerySql, StatementPreparer statementPreparer, ResultSetMapper<T> mapper) {
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
            } catch (SQLServerException e) {
                // 为过程或函数 cdc.fn_cdc_get_all_changes_ ...  提供的参数数目不足。
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                close(rs);
                close(ps);
            }
            return apply;
        });
        return (T) execute;
    }

    /**
     * 获取最大 LSN（同时考虑 DML 和 DDL）
     * <p>
     * 问题说明：
     * - sys.fn_cdc_get_max_lsn() 只返回变更表（change tables）中的最大 LSN，不包括 DDL 的 LSN
     * - 当只有 DDL 发生而没有 DML 变更时，sys.fn_cdc_get_max_lsn() 不会变化
     * - 这导致 LsnPuller 无法检测到 DDL 变更，从而无法触发 pull() 方法
     * <p>
     * 解决方案：
     * - 同时查询 DML 和 DDL 的最大 LSN，取两者中的较大值
     * - 确保 DDL 变更也能被及时检测和处理
     */
    public Lsn getMaxLsn() {
        // 获取 DML 的最大 LSN
        Lsn dmlMaxLsn = queryAndMap(GET_MAX_LSN, rs -> {
            byte[] bytes = rs.getBytes(1);
            return bytes != null ? new Lsn(bytes) : null;
        });

        // 获取 DDL 的最大 LSN（可能为 null，如果 cdc.ddl_history 表为空或不存在）
        Lsn ddlMaxLsn = queryAndMap(GET_MAX_DDL_LSN, rs -> {
            byte[] bytes = rs.getBytes(1);
            return bytes != null ? new Lsn(bytes) : null;
        });

        // 返回两者中的较大值
        if (dmlMaxLsn == null && ddlMaxLsn == null) {
            return null;
        }
        if (dmlMaxLsn == null) {
            return ddlMaxLsn;
        }
        if (ddlMaxLsn == null) {
            return dmlMaxLsn;
        }

        return dmlMaxLsn.compareTo(ddlMaxLsn) > 0 ? dmlMaxLsn : ddlMaxLsn;
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    Lsn stopLsn = buffer.take();
                    Lsn poll;
                    while ((poll = buffer.poll()) != null) {
                        stopLsn = poll;
                    }
                    if (!stopLsn.isAvailable() || stopLsn.compareTo(lastLsn) <= 0) {
                        continue;
                    }

                    pull(stopLsn);

                    // LSN 已在 parseUnifiedEvents() 中统一更新，这里不需要重复更新
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

    public Lsn getLastLsn() {
        return lastLsn;
    }

    public void pushStopLsn(Lsn stopLsn) {
        if (buffer.contains(stopLsn)) {
            return;
        }
        if (!buffer.offer(stopLsn)) {
            try {
                lock.lock();
                while (!buffer.offer(stopLsn) && connected) {
                    logger.warn("[{}]缓存队列容量已达上限[{}], 正在阻塞重试.", this.getClass().getSimpleName(), BUFFER_CAPACITY);
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
}