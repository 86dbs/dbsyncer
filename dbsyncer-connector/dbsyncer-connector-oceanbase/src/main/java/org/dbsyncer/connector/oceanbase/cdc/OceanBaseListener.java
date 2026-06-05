/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbase.OceanBaseException;
import org.dbsyncer.connector.oceanbase.binlog.BinaryLogClient;
import org.dbsyncer.connector.oceanbase.binlog.BinaryLogRemoteClient;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.util.SqlParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OceanBase 增量监听，基于 Binlog 服务输出的 MySQL Binlog V4 协议
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public class OceanBaseListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String BINLOG_FILENAME = "fileName";
    private final String BINLOG_POSITION = "position";
    private final Map<Long, TableMapEventData> tables = new HashMap<>();
    private BinaryLogClient client;
    private final Lock connectLock = new ReentrantLock();

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (client != null && client.isConnected()) {
                logger.error("OceanBase Binlog 监听器已启动");
                return;
            }
            run();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new OceanBaseException(e);
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
    public Map<String, String> captureSnapshot() {
        try {
            final DatabaseConfig config = getConnectorInstance().getConfig();
            BinaryLogRemoteClient captureClient = createBinlogClient(config);
            captureClient.connect();
            refreshSnapshot(captureClient.getBinlogFilename(), captureClient.getBinlogPosition());
            captureClient.disconnect();
            Map<String, String> captured = new HashMap<>(2);
            captured.put(BINLOG_FILENAME, snapshot.get(BINLOG_FILENAME));
            captured.put(BINLOG_POSITION, snapshot.get(BINLOG_POSITION));
            return captured;
        } catch (Exception e) {
            logger.error("捕获 OceanBase Binlog 位点失败:{}", e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (StringUtil.isBlank(offset.getNextFileName()) && offset.getPosition() == null) {
            return;
        }
        refreshSnapshot(offset.getNextFileName(), (Long) offset.getPosition());
    }

    private BinaryLogRemoteClient createBinlogClient(DatabaseConfig config) throws Exception {
        String host = OceanBaseBinlogConfig.getBinlogHost(config);
        int port = OceanBaseBinlogConfig.getBinlogPort(config);
        logger.info("连接 OceanBase Binlog 服务 {}:{}", host, port);
        return new BinaryLogRemoteClient(host, port, config.getUsername(), config.getPassword());
    }

    private void run() throws Exception {
        final DatabaseConfig config = getConnectorInstance().getConfig();
        boolean containsPos = snapshot.containsKey(BINLOG_POSITION);
        client = createBinlogClient(config);
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

    private void trySendEvent(ChangedEvent event) {
        try {
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

    final class InnerLifecycleListener implements BinaryLogRemoteClient.LifecycleListener {

        @Override
        public void onConnect(BinaryLogRemoteClient client) {
            refresh(client.getBinlogFilename(), client.getBinlogPosition());
        }

        @Override
        public void onException(BinaryLogRemoteClient client, Exception e) {
            if (!client.isConnected()) {
                return;
            }
            if (e instanceof ServerException) {
                ServerException serverException = (ServerException) e;
                if (serverException.getErrorCode() == 1236) {
                    String log = String.format("[%s]执行异常，建议重新保存驱动，再启动驱动。", client.getWorkerThreadName());
                    errorEvent(new OceanBaseException(log + e.getMessage()));
                    return;
                }
            }
            errorEvent(new OceanBaseException(e.getMessage()));
        }

        @Override
        public void onDisconnect(BinaryLogRemoteClient client) {
        }
    }

    final class InnerEventListener implements BinaryLogRemoteClient.EventListener {

        private boolean notUniqueCodeEvent = true;

        @Override
        public void onEvent(Event event) {
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
                if (!StringUtil.equalsIgnoreCase("BEGIN", sql)) {
                    return SqlParserUtil.parse(sql);
                }
            } catch (JSQLParserException e) {
                logger.warn("不支持的ddl:{}", sql);
            }
            return null;
        }

        private void parseAlter(QueryEventData data, Alter alter) {
            String tableName = StringUtil.replace(alter.getTable().getName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            String databaseName = alter.getTable().getSchemaName();
            if (StringUtil.isBlank(databaseName)) {
                databaseName = data.getDatabase();
            }
            databaseName = StringUtil.replace(databaseName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            if (isFilterTable(databaseName, tableName)) {
                trySendEvent(new DDLChangedEvent(tableName, ConnectorConstant.OPERTION_ALTER, data.getSql(), client.getBinlogFilename(), client.getBinlogPosition()));
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

        private boolean isNotUniqueCodeEvent(String sql) {
            return !StringUtil.startsWith(sql, DatabaseConstant.DBS_UNIQUE_CODE);
        }
    }
}
