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
    public void start() throws Exception {
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

    private void connect() throws Exception {
        instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;
        if (service.isAlive(instance)) {
            DatabaseConfig cfg = instance.getConfig();
            serverName = cfg.getUrl();
            schema = cfg.getSchema();
        }
    }

    private void readLastLsn() throws Exception {
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

    private void readTables() throws Exception {
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

    private void readChangeTables() throws Exception {
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
                boolean enabledTableCDC = false;
                try {
                    enabledTableCDC = queryAndMap(IS_TABLE_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, table), rs -> rs.getInt(1) > 0);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (!enabledTableCDC) {
                    try {
                        execute(String.format(ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, table), schema));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    Lsn minLsn = null;
                    try {
                        minLsn = queryAndMap(GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, table), rs -> new Lsn(rs.getBytes(1)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    logger.info("启用CDC表[{}]:{}", table, minLsn.isAvailable());
                }
            });
        }
    }

    private void enableDBCDC() throws Exception {
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

    private void execute(String... sqlStatements) throws Exception {
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

    private void pull(Lsn stopLsn) throws Exception {
        Lsn startLsn = queryAndMap(GET_INCREMENT_LSN, statement -> statement.setBytes(1, lastLsn.getBinary()), rs -> Lsn.valueOf(rs.getBytes(1)));

        // 查询 DML 变更（提取 LSN 用于排序）
        List<DMLEvent> dmlEvents = new ArrayList<>();
        changeTables.forEach(changeTable -> {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());

            List<DMLEvent> list = null;
            try {
                list = queryAndMapList(query, statement -> {
                    statement.setBytes(1, startLsn.getBinary());
                    statement.setBytes(2, stopLsn.getBinary());
                }, rs -> {
                    int columnCount = rs.getMetaData().getColumnCount();
                    List<Object> row = null;
                    List<DMLEvent> data = new ArrayList<>();

                    while (rs.next()) {
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
                        DMLEvent DMLEvent = new DMLEvent(changeTable.getTableName(), operation, row, eventLsn,
                                new ArrayList<>(changeTable.getCapturedColumnList()));
                        data.add(DMLEvent);
                    }
                    return data;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

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
    private List<DDLEvent> pullDDLChanges(Lsn startLsn, Lsn stopLsn) throws Exception {
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
     * 重新启用表的 CDC（用于在列结构变更后更新捕获列列表）
     * <p>
     * 适用场景：
     * - ADD COLUMN: 需要捕获新列
     * - DROP COLUMN: 需要移除已删除的列
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
    public Lsn getMaxLsn() throws Exception {
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