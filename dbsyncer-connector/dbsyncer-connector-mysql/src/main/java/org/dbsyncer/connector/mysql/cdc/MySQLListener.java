/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.cdc;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.network.ServerException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.MySQLException;
import org.dbsyncer.connector.mysql.binlog.BinaryLogClient;
import org.dbsyncer.connector.mysql.binlog.BinaryLogRemoteClient;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.regex.Pattern.compile;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-28 22:02
 */
public class MySQLListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String BINLOG_FILENAME = "fileName";
    private final String BINLOG_POSITION = "position";
    private final Map<Long, TableMapEventData> tables = new HashMap<>();
    // 缓存表的列名信息：tableId -> 列名列表（按数据顺序）
    private final Map<Long, List<String>> tableColumnNames = new HashMap<>();
    private BinaryLogClient client;
    private String database;
    private final Lock connectLock = new ReentrantLock();

    @Override
    public void start() throws Exception {
        try {
            connectLock.lock();
            if (client != null && client.isConnected()) {
                logger.error("MySQLExtractor is already started");
                return;
            }
            run();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new MySQLException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            connectLock.lock();
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        refreshSnapshot(offset.getNextFileName(), (Long) offset.getPosition());
    }

    private void run() throws Exception {
        final DatabaseConfig config = getConnectorInstance().getConfig();
        if (StringUtil.isBlank(config.getUrl())) {
            throw new MySQLException("url is invalid");
        }
        database = DatabaseUtil.getDatabaseName(config.getUrl());
        List<Host> cluster = readNodes(config.getUrl());
        Assert.notEmpty(cluster, "MySQL连接地址有误.");

        int MASTER = 0;
        final Host host = cluster.get(MASTER);
        final String username = config.getUsername();
        final String password = config.getPassword();
        boolean containsPos = snapshot.containsKey(BINLOG_POSITION);
        client = new BinaryLogRemoteClient(host.getIp(), host.getPort(), username, password);
        client.setBinlogFilename(snapshot.get(BINLOG_FILENAME));
        client.setBinlogPosition(containsPos ? Long.parseLong(snapshot.get(BINLOG_POSITION)) : 0);
        client.setTableMapEventByTableId(tables);
        client.registerEventListener(new InnerEventListener());
        client.registerLifecycleListener(new InnerLifecycleListener());

        client.connect();

        if (!containsPos) {
            refreshSnapshot(client.getBinlogFilename(), client.getBinlogPosition());
            super.forceFlushEvent();
        }
    }

    private List<Host> readNodes(String url) {
        Matcher matcher = compile("(//)(?!(/)).+?(/)").matcher(url);
        if (matcher.find()) {
            url = matcher.group(0);
        }
        url = StringUtil.replace(url, "/", "");

        List<Host> cluster = new ArrayList<>();
        String[] arr = StringUtil.split(url, StringUtil.COMMA);
        for (String s : arr) {
            String[] host = StringUtil.split(s, ":");
            if (2 == host.length) {
                cluster.add(new Host(host[0], Integer.parseInt(host[1])));
            }
        }
        return cluster;
    }

    private void refresh(EventHeader header) {
        EventHeaderV4 eventHeaderV4 = (EventHeaderV4) header;
        refresh(null, eventHeaderV4.getNextPosition());
    }

    private void refresh(String binlogFilename, long nextPosition) {
        if (StringUtil.isNotBlank(binlogFilename)) {
            client.setBinlogFilename(binlogFilename);
        }
        if (0 < nextPosition) {
            client.setBinlogPosition(nextPosition);
        }
        // 即使没有同步数据，binlog 也在继续前进，需要更新 snapshot 以保持位置同步
        refreshSnapshot(client.getBinlogFilename(), client.getBinlogPosition());
    }

    private void refreshSnapshot(String binlogFilename, long nextPosition) {
        snapshot.put(BINLOG_FILENAME, binlogFilename);
        snapshot.put(BINLOG_POSITION, String.valueOf(nextPosition));
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            // 如果消费事件失败，重试
            while (client.isConnected()) {
                try {
                    sendChangedEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    static final class Host {
        private final String ip;
        private final int port;

        public Host(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }
    }

    final class InnerLifecycleListener implements BinaryLogRemoteClient.LifecycleListener {

        @Override
        public void onConnect(BinaryLogRemoteClient client) {
            // 记录binlog增量点
            refresh(client.getBinlogFilename(), client.getBinlogPosition());
        }

        @Override
        public void onException(BinaryLogRemoteClient client, Exception e) {
            if (!client.isConnected()) {
                return;
            }
            /**
             * e:
             * case1> Due to the automatic expiration and deletion mechanism of MySQL binlog files, the binlog file cannot be found.
             * case2> Got fatal error 1236 from master when reading data from binary log.
             * case3> Log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master.
             */
            if (e instanceof ServerException) {
                ServerException serverException = (ServerException) e;
                if (serverException.getErrorCode() == 1236) {
                    String log = String.format("线程[%s]执行异常。由于MySQL配置了过期binlog文件自动删除机制，已无法找到原binlog文件%s。建议先保存驱动（加载最新的binlog文件），再启动驱动。",
                            client.getWorkerThreadName(),
                            client.getBinlogFilename());
                    errorEvent(new MySQLException(log));
                    close();
                    return;
                }
            }
            errorEvent(new MySQLException(e.getMessage()));
        }

        @Override
        public void onDisconnect(BinaryLogRemoteClient client) {
        }

    }

    final class InnerEventListener implements BinaryLogRemoteClient.EventListener {

        /**
         * 只处理非dbs写入事件（单线程消费，不存在并发竞争）
         */
        private boolean notUniqueCodeEvent = true;

        @Override
        public void onEvent(Event event) {
            // ROTATE > FORMAT_DESCRIPTION > TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID
            EventHeader header = event.getHeader();
            if (header.getEventType() == EventType.XID) {
                refresh(header);
                return;
            }

            // 处理 TABLE_MAP 事件：主动更新列名缓存
            if (header.getEventType() == EventType.TABLE_MAP) {
                TableMapEventData data = (TableMapEventData) event.getData();
                if (data != null && isFilterTable(data.getDatabase(), data.getTable())) {
                    // 如果是我们监控的表，主动更新列名缓存
                    updateColumnNamesCache(data.getTableId(), data.getTable());
                }
                return;
            }

            if (header.getEventType() == EventType.ROWS_QUERY) {
                RowsQueryEventData data = event.getData();
                notUniqueCodeEvent = isNotUniqueCodeEvent(data.getQuery());
                return;
            }

            if (notUniqueCodeEvent && EventType.isUpdate(header.getEventType())) {
                refresh(header);
                UpdateRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    long tableId = data.getTableId();
                    List<String> columnNames = getColumnNames(tableId);
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m.getValue()).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(tableId), ConnectorConstant.OPERTION_UPDATE, after, client.getBinlogFilename(), client.getBinlogPosition(), columnNames));
                    });
                }
                return;
            }
            if (notUniqueCodeEvent && EventType.isWrite(header.getEventType())) {
                refresh(header);
                WriteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    long tableId = data.getTableId();
                    List<String> columnNames = getColumnNames(tableId);
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(tableId), ConnectorConstant.OPERTION_INSERT, after, client.getBinlogFilename(), client.getBinlogPosition(), columnNames));
                    });
                }
                return;
            }
            if (notUniqueCodeEvent && EventType.isDelete(header.getEventType())) {
                refresh(header);
                DeleteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    long tableId = data.getTableId();
                    List<String> columnNames = getColumnNames(tableId);
                    data.getRows().forEach(m -> {
                        List<Object> before = Stream.of(m).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(tableId), ConnectorConstant.OPERTION_DELETE, before, client.getBinlogFilename(), client.getBinlogPosition(), columnNames));
                    });
                }
                return;
            }

            if (EventType.QUERY == header.getEventType()) {
                refresh(header);
                parseDDL(event.getData());
                return;
            }

            // 切换binlog
            if (header.getEventType() == EventType.ROTATE) {
                RotateEventData data = event.getData();
                refresh(data.getBinlogFilename(), data.getBinlogPosition());
            }
        }

        private void parseDDL(QueryEventData data) {
            if (isNotUniqueCodeEvent(data.getSql())) {
                Statement statement = parseStatement(data.getSql());
                if (statement instanceof Alter) {
                    parseAlter(data, (Alter) statement);
                }
            }
        }

        private Statement parseStatement(String sql) {
            try {
                // skip BEGIN
                if (!StringUtil.equalsIgnoreCase("BEGIN", sql)) {
                    return CCJSqlParserUtil.parse(sql);
//                    return SqlParserUtil.parse(sql); 太过于复杂，能能差
                }
            } catch (JSQLParserException e) {
                logger.warn("不支持的ddl:{}", sql);
            }
            return null;
        }

        private void parseAlter(QueryEventData data, Alter alter) {
            // ALTER TABLE `test`.`my_user` MODIFY COLUMN `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL AFTER `id`
            String tableName = StringUtil.replace(alter.getTable().getName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            // databaseName 的取值不能为 QueryEventData#getDatabase, getDatabase 获取的是执行 SQL 语句时所在的数据库上下文,
            // 不是 alter 语句修改的表所在的数据库
            // 依次执行下面两条语句 use db1; alter table db2.my_user add column name varchar(128);
            // data.getDatabase() 的值为 db1, 不是 db2, 必须从 alter 中获取 databaseName
            String databaseName = alter.getTable().getSchemaName();
            // alter 中没有 databaseName 说明是 alter table my_user xxx 这种格式, 直接在上下文中获取即可
            if (StringUtil.isBlank(databaseName)) {
                databaseName = data.getDatabase();
            }
            databaseName = StringUtil.replace(databaseName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            if (isFilterTable(databaseName, tableName)) {
                // DDL 事件会改变表结构，清除对应表的列名缓存
                clearColumnNamesCache(tableName);
                logger.info("捕获到 DDL 事件: database={}, table={}, sql={}", databaseName, tableName, data.getSql());
                trySendEvent(new DDLChangedEvent(tableName, ConnectorConstant.OPERTION_ALTER,
                        data.getSql(), client.getBinlogFilename(), client.getBinlogPosition()));
            }
        }

        private String getTableName(long tableId) {
            return tables.get(tableId).getTable();
        }

        /**
         * 获取表的列名列表（按数据顺序）
         * 从缓存中获取，如果不存在则从数据库元数据中获取并缓存
         */
        private List<String> getColumnNames(long tableId) {
            // 先从缓存中获取
            List<String> columnNames = tableColumnNames.get(tableId);
            if (columnNames != null) {
                return columnNames;
            }
            
            // 如果缓存中没有，从数据库元数据中获取并缓存
            TableMapEventData tableMap = tables.get(tableId);
            if (tableMap != null) {
                String tableName = tableMap.getTable();
                columnNames = updateColumnNamesCache(tableId, tableName);
            }
            
            return columnNames;
        }

        /**
         * 更新表的列名缓存
         * 
         * @param tableId 表ID
         * @param tableName 表名
         * @return 列名列表
         */
        private List<String> updateColumnNamesCache(long tableId, String tableName) {
            try {
                // 从数据库元数据中获取列名
                List<String> columnNames = getTableColumnNames(tableName);
                if (columnNames != null && !columnNames.isEmpty()) {
                    // 缓存列名信息
                    tableColumnNames.put(tableId, columnNames);
                }
                return columnNames;
            } catch (Exception e) {
                logger.warn("更新表列名缓存失败，tableId: {}, tableName: {}, error: {}", tableId, tableName, e.getMessage());
                return null;
            }
        }

        /**
         * 清除表的列名缓存（DDL 事件后调用）
         * 
         * @param tableName 表名
         */
        private void clearColumnNamesCache(String tableName) {
            // 根据表名找到对应的 tableId，清除缓存
            // 注意：同一个表名可能对应多个 tableId（如果表被删除后重建），所以需要遍历所有 tableId
            List<Long> tableIdsToRemove = new ArrayList<>();
            for (Map.Entry<Long, TableMapEventData> entry : tables.entrySet()) {
                if (entry.getValue() != null && tableName.equals(entry.getValue().getTable())) {
                    tableIdsToRemove.add(entry.getKey());
                }
            }
            
            // 清除缓存
            for (Long tableId : tableIdsToRemove) {
                tableColumnNames.remove(tableId);
            }
        }

        /**
         * 从数据库元数据中获取表的列名列表
         */
        private List<String> getTableColumnNames(String tableName) {
            try {
                return getConnectorInstance().execute(databaseTemplate -> {
                    List<String> columns = new ArrayList<>();
                    try (java.sql.ResultSet rs = databaseTemplate.getSimpleConnection().getConnection()
                            .getMetaData().getColumns(null, database, tableName, null)) {
                        while (rs.next()) {
                            columns.add(rs.getString("COLUMN_NAME"));
                        }
                    }
                    return columns;
                });
            } catch (Exception e) {
                logger.error("从数据库元数据获取列名失败，tableName: {}, error: {}", tableName, e.getMessage());
                return null;
            }
        }

        private boolean isFilterTable(long tableId) {
            final TableMapEventData tableMap = tables.get(tableId);
            return isFilterTable(tableMap.getDatabase(), tableMap.getTable());
        }

        private boolean isFilterTable(String dbName, String tableName) {
            return StringUtil.equalsIgnoreCase(database, dbName) && filterTable.contains(tableName);
        }

        private boolean isNotUniqueCodeEvent(String sql) {
            return !StringUtil.startsWith(sql, "/*dbs*/");
        }

    }
}