/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.cdc;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
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
import org.dbsyncer.sdk.constant.DatabaseConstant;
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
    private final int MASTER = 0;
    private Map<Long, TableMapEventData> tables = new HashMap<>();
    private BinaryLogClient client;
    private List<Host> cluster;
    private String database;
    private final Lock connectLock = new ReentrantLock();

    @Override
    public void start() {
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
        cluster = readNodes(config.getUrl());
        Assert.notEmpty(cluster, "MySQL连接地址有误.");

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
        String[] arr = StringUtil.split(url, ",");
        int size = arr.length;
        for (int i = 0; i < size; i++) {
            String[] host = StringUtil.split(arr[i], ":");
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
    }

    private void refreshSnapshot(String binlogFilename, long nextPosition) {
        snapshot.put(BINLOG_FILENAME, binlogFilename);
        snapshot.put(BINLOG_POSITION, String.valueOf(nextPosition));
    }

    private void trySendEvent(ChangedEvent event){
        try {
            // 如果消费事件失败，重试
            while (client.isConnected()){
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

    final class Host {
        private String ip;
        private int port;

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

            if (header.getEventType() == EventType.ROWS_QUERY) {
                RowsQueryEventData data = event.getData();
                notUniqueCodeEvent = isNotUniqueCodeEvent(data.getQuery());
                return;
            }

            if (notUniqueCodeEvent && EventType.isUpdate(header.getEventType())) {
                refresh(header);
                UpdateRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m.getValue()).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_UPDATE, after, client.getBinlogFilename(), client.getBinlogPosition()));
                    });
                }
                return;
            }
            if (notUniqueCodeEvent && EventType.isWrite(header.getEventType())) {
                refresh(header);
                WriteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_INSERT, after, client.getBinlogFilename(), client.getBinlogPosition()));
                    });
                }
                return;
            }
            if (notUniqueCodeEvent && EventType.isDelete(header.getEventType())) {
                refresh(header);
                DeleteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> before = Stream.of(m).collect(Collectors.toList());
                        trySendEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_DELETE, before, client.getBinlogFilename(), client.getBinlogPosition()));
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
            if (isNotUniqueCodeEvent(data.getSql()) && StringUtil.contains(data.getSql().toUpperCase(), ConnectorConstant.OPERTION_ALTER)) {
                Alter alter = parseAlter(data.getSql());
                if (alter == null) {
                    return;
                }

                // ALTER TABLE `test`.`my_user` MODIFY COLUMN `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL AFTER `id`
                String tableName = StringUtil.replace(alter.getTable().getName(), "`", "");
                // databaseName 的取值不能为 QueryEventData#getDatabase, getDatabase 获取的是执行 SQL 语句时所在的数据库上下文,
                // 不是 alter 语句修改的表所在的数据库
                // 依次执行下面两条语句 use db1; alter table db2.my_user add column name varchar(128);
                // data.getDatabase() 的值为 db1, 不是 db2, 必须从 alter 中获取 databaseName
                String databaseName = alter.getTable().getSchemaName();
                if (isFilterTable(databaseName, tableName)) {
                    logger.info("sql:{}", data.getSql());
                    trySendEvent(new DDLChangedEvent(databaseName, tableName, ConnectorConstant.OPERTION_ALTER, data.getSql(), client.getBinlogFilename(), client.getBinlogPosition()));
                }
            }
        }

        private Alter parseAlter(String sql) {
            try {
                Statement statement = CCJSqlParserUtil.parse(sql);
                if (statement instanceof Alter) {
                    return (Alter) statement;
                } else {
                    return null;
                }
            } catch (JSQLParserException e) {
                logger.error("不支持ddl sql，支持标准的sql格式，请查看文档https://gitee.com/ghi/dbsyncer/wikis/%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8C/%E8%A1%A8%E7%BB%93%E6%9E%84%E5%90%8C%E6%AD%A5");
                return null;
            }
        }

        private String getTableName(long tableId) {
            return tables.get(tableId).getTable();
        }

        private boolean isFilterTable(long tableId) {
            final TableMapEventData tableMap = tables.get(tableId);
            return isFilterTable(tableMap.getDatabase(), tableMap.getTable());
        }

        private boolean isFilterTable(String dbName, String tableName) {
            return StringUtil.equalsIgnoreCase(database, dbName) && filterTable.contains(tableName);
        }

        private boolean isNotUniqueCodeEvent(String sql){
            return !StringUtil.startsWith(sql, DatabaseConstant.DBS_UNIQUE_CODE);
        }

    }
}