package org.dbsyncer.connector.mysql;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.network.ServerException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.AbstractDatabaseListener;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
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
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class MySQLListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String BINLOG_FILENAME = "fileName";
    private final String BINLOG_POSITION = "position";
    private final int RETRY_TIMES = 10;
    private final int MASTER = 0;
    private Map<Long, TableMapEventData> tables = new HashMap<>();
    private BinaryLogClient client;
    private List<Host> cluster;
    private String database;
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private volatile boolean recovery;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("MySQLExtractor is already started");
                return;
            }
            run();
            connected = true;
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ConnectorException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            connectLock.lock();
            connected = false;
            if (null != client) {
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
        final DatabaseConfig config = (DatabaseConfig) connectorConfig;
        if (StringUtil.isBlank(config.getUrl())) {
            throw new ConnectorException("url is invalid");
        }
        database = DatabaseUtil.getDatabaseName(config.getUrl());
        cluster = readNodes(config.getUrl());
        Assert.notEmpty(cluster, "MySQL连接地址有误.");

        final Host host = cluster.get(MASTER);
        final String username = config.getUsername();
        final String password = config.getPassword();
        boolean containsPos = snapshot.containsKey(BINLOG_POSITION);
        client = new BinaryLogRemoteClient(host.getIp(), host.getPort(), username, password);
        client.setEnableDDL(true);
        client.setBinlogFilename(snapshot.get(BINLOG_FILENAME));
        client.setBinlogPosition(containsPos ? Long.parseLong(snapshot.get(BINLOG_POSITION)) : 0);
        client.setTableMapEventByTableId(tables);
        client.registerEventListener(new MysqlEventListener());
        client.registerLifecycleListener(new MysqlLifecycleListener());

        client.connect();

        if (!containsPos) {
            refreshSnapshot(client.getBinlogFilename(), client.getBinlogPosition());
            super.forceFlushEvent();
        }
    }

    private List<Host> readNodes(String url) {
        Matcher matcher = compile("(//)(?!(/)).+?(/)").matcher(url);
        while (matcher.find()) {
            url = matcher.group(0);
            break;
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

    private void reStart() {
        try {
            connectLock.lock();
            if (recovery) {
                return;
            }
            recovery = true;
        } finally {
            connectLock.unlock();
        }

        for (int i = 1; i <= RETRY_TIMES; i++) {
            try {
                if (null != client) {
                    client.disconnect();
                }
                run();

                errorEvent(new ConnectorException(String.format("重启成功, %s", client.getWorkerThreadName())));
                logger.error("第{}次重启成功, ThreadName:{} ", i, client.getWorkerThreadName());
                recovery = false;
                break;
            } catch (Exception e) {
                logger.error("第{}次重启异常, ThreadName:{}, {}", i, client.getWorkerThreadName(), e.getMessage());
                // 无法连接，关闭任务
                if (i == RETRY_TIMES) {
                    errorEvent(new ConnectorException(String.format("重启异常, %s, %s", client.getWorkerThreadName(), e.getMessage())));
                }
            }
            try {
                TimeUnit.SECONDS.sleep(i * 2);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
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

    final class MysqlLifecycleListener implements BinaryLogRemoteClient.LifecycleListener {

        @Override
        public void onConnect(BinaryLogRemoteClient client) {
            // 记录binlog增量点
            refresh(client.getBinlogFilename(), client.getBinlogPosition());
        }

        @Override
        public void onCommunicationFailure(BinaryLogRemoteClient client, Exception e) {
            if (!connected) {
                return;
            }
            logger.error(e.getMessage());
            /**
             * e:
             * case1> Due to the automatic expiration and deletion mechanism of MySQL binlog files, the binlog file cannot be found.
             * case2> Got fatal error 1236 from master when reading data from binary log.
             * case3> Log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master.
             */
            if (e instanceof ServerException) {
                ServerException serverException = (ServerException) e;
                if (serverException.getErrorCode() == 1236) {
                    close();
                    String log = String.format("线程[%s]执行异常。由于MySQL配置了过期binlog文件自动删除机制，已无法找到原binlog文件%s。建议先保存驱动（加载最新的binlog文件），再启动驱动。",
                            client.getWorkerThreadName(),
                            client.getBinlogFilename());
                    errorEvent(new ConnectorException(log));
                    return;
                }
            }

            reStart();
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogRemoteClient client, Exception ex) {
        }

        @Override
        public void onDisconnect(BinaryLogRemoteClient client) {
        }

    }

    final class MysqlEventListener implements BinaryLogRemoteClient.EventListener {

        @Override
        public void onEvent(Event event) {
            // ROTATE > FORMAT_DESCRIPTION > TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID
            EventHeader header = event.getHeader();
            if (header.getEventType() == EventType.XID) {
                refresh(header);
                return;
            }

            if (EventType.isUpdate(header.getEventType())) {
                refresh(header);
                UpdateRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m.getValue()).collect(Collectors.toList());
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_UPDATE, after, client.getBinlogFilename(), client.getBinlogPosition()));
                    });
                }
                return;
            }
            if (EventType.isWrite(header.getEventType())) {
                refresh(header);
                WriteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> after = Stream.of(m).collect(Collectors.toList());
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_INSERT, after, client.getBinlogFilename(), client.getBinlogPosition()));
                    });
                }
                return;
            }
            if (EventType.isDelete(header.getEventType())) {
                refresh(header);
                DeleteRowsEventData data = event.getData();
                if (isFilterTable(data.getTableId())) {
                    data.getRows().forEach(m -> {
                        List<Object> before = Stream.of(m).collect(Collectors.toList());
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_DELETE, before, client.getBinlogFilename(), client.getBinlogPosition()));
                    });
                }
                return;
            }

            if (client.isEnableDDL() && EventType.QUERY == header.getEventType()) {
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
            if (StringUtil.startsWith(data.getSql(), ConnectorConstant.OPERTION_ALTER)) {
                try {
                    // ALTER TABLE `test`.`my_user` MODIFY COLUMN `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL AFTER `id`
                    Alter alter = (Alter) CCJSqlParserUtil.parse(data.getSql());
                    String tableName = StringUtil.replace(alter.getTable().getName(), "`", "");
                    if (isFilterTable(data.getDatabase(), tableName)) {
                        logger.info("sql:{}", data.getSql());
                        changeEvent(new DDLChangedEvent(data.getDatabase(), tableName, ConnectorConstant.OPERTION_ALTER, data.getSql(), client.getBinlogFilename(), client.getBinlogPosition()));
                    }
                } catch (JSQLParserException e) {
                    logger.error("不支持ddl sql，支持标准的sql格式，请查看文档https://gitee.com/ghi/dbsyncer/wikis/%E5%BF%AB%E9%80%9F%E4%BA%86%E8%A7%A3/%E8%A1%A8%E7%BB%93%E6%9E%84%E5%90%8C%E6%AD%A5");
                }
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

    }

}