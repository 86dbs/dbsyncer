package org.dbsyncer.listener.mysql;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.network.ServerException;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.AbstractDatabaseExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.config.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
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
public class MysqlExtractor extends AbstractDatabaseExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String BINLOG_FILENAME = "fileName";
    private static final String BINLOG_POSITION = "position";
    private static final int RETRY_TIMES = 10;
    private static final int MASTER = 0;
    private Map<Long, TableMapEventData> tables = new HashMap<>();
    private BinaryLogClient client;
    private List<Host> cluster;
    private String database;
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("MysqlExtractor is already started");
                return;
            }
            run();
            connected = true;
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
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
    protected void sendChangedEvent(RowChangedEvent event) {
        changedEvent(event);
    }

    private void run() throws Exception {
        final DatabaseConfig config = (DatabaseConfig) connectorConfig;
        if (StringUtil.isBlank(config.getUrl())) {
            throw new ListenerException("url is invalid");
        }
        database = readDatabaseName(config.getUrl());
        cluster = readNodes(config.getUrl());
        Assert.notEmpty(cluster, "Mysql连接地址有误.");

        final Host host = cluster.get(MASTER);
        final String username = config.getUsername();
        final String password = config.getPassword();
        final String pos = snapshot.get(BINLOG_POSITION);
        client = new BinaryLogRemoteClient(host.getIp(), host.getPort(), username, password);
        client.setBinlogFilename(snapshot.get(BINLOG_FILENAME));
        client.setBinlogPosition(StringUtil.isBlank(pos) ? 0 : Long.parseLong(pos));
        client.setTableMapEventByTableId(tables);
        client.registerEventListener(new MysqlEventListener());
        client.registerLifecycleListener(new MysqlLifecycleListener());

        client.connect();
    }

    private String readDatabaseName(String url) {
        Matcher matcher = compile("(//)(?!(\\?)).+?(\\?)").matcher(url);
        while (matcher.find()) {
            url = matcher.group(0);
            break;
        }
        int s = url.lastIndexOf("/");
        int e = url.lastIndexOf("?");
        if (s > 0 && e > 0) {
            return StringUtil.substring(url, s + 1, e);
        }

        throw new ListenerException("database is invalid");
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
        for (int i = 1; i <= RETRY_TIMES; i++) {
            try {
                if (null != client) {
                    client.disconnect();
                }
                run();

                errorEvent(new ListenerException(String.format("重启成功, %s", client.getWorkerThreadName())));
                logger.error("第{}次重启成功, ThreadName:{} ", i, client.getWorkerThreadName());
                break;
            } catch (Exception e) {
                logger.error("第{}次重启异常, ThreadName:{}, {}", i, client.getWorkerThreadName(), e.getMessage());
                // 无法连接，关闭任务
                if (i == RETRY_TIMES) {
                    interruptException(new ListenerException(String.format("重启异常, %s, %s", client.getWorkerThreadName(), e.getMessage())));
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
            snapshot.put(BINLOG_FILENAME, binlogFilename);
        }
        if (0 < nextPosition) {
            client.setBinlogPosition(nextPosition);
            snapshot.put(BINLOG_POSITION, String.valueOf(nextPosition));
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
                    interruptException(new ListenerException(log));
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
                        List<Object> before = Stream.of(m.getKey()).collect(Collectors.toList());
                        List<Object> after = Stream.of(m.getValue()).collect(Collectors.toList());
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_UPDATE, before, after));
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
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, after));
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
                        sendChangedEvent(new RowChangedEvent(getTableName(data.getTableId()), ConnectorConstant.OPERTION_DELETE, before, Collections.EMPTY_LIST));
                    });
                }
                return;
            }

            // 切换binlog
            if (header.getEventType() == EventType.ROTATE) {
                RotateEventData data = event.getData();
                refresh(data.getBinlogFilename(), data.getBinlogPosition());
                forceFlushEvent();
                return;
            }
        }

        private String getTableName(long tableId) {
            return tables.get(tableId).getTable();
        }

        private boolean isFilterTable(long tableId) {
            final TableMapEventData tableMap = tables.get(tableId);
            return StringUtil.equals(database, tableMap.getDatabase()) && filterTable.contains(tableMap.getTable());
        }

    }

}