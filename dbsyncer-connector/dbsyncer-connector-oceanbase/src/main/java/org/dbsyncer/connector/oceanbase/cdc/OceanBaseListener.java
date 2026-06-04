/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbase.OceanBaseException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * OceanBase 增量监听，基于 LogProxyClient
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public class OceanBaseListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Lock connectLock = new ReentrantLock();
    private final List<LogMessage> logMessageBuffer = new LinkedList<>();
    private final Map<String, Table> tableMap = new HashMap<>();

    private volatile boolean running;
    private LogProxyClient client;
    private Worker worker;
    private String safeTimestamp;

    @Override
    public void start() {
        connectLock.lock();
        try {
            if (running) {
                logger.warn("OceanBase LogProxy 监听器已启动");
                return;
            }
            initTableMap();
            running = true;
            worker = new Worker();
            worker.setName("oceanbase-logproxy-" + hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            running = false;
            logger.error("启动 OceanBase LogProxy 监听器失败: {}", e.getMessage(), e);
            throw new OceanBaseException(e);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close() {
        connectLock.lock();
        try {
            running = false;
            if (client != null) {
                try {
                    client.stop();
                } catch (Exception e) {
                    logger.warn("停止 LogProxyClient 异常: {}", e.getMessage());
                }
            }
            if (worker != null && worker.isAlive()) {
                worker.interrupt();
                try {
                    worker.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            logMessageBuffer.clear();
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public Map<String, String> captureSnapshot() {
        Map<String, String> captured = new HashMap<>(1);
        String timestamp = snapshot.get(OceanBaseCdcConstants.SNAPSHOT_TIMESTAMP);
        captured.put(OceanBaseCdcConstants.SNAPSHOT_TIMESTAMP, StringUtil.isBlank(timestamp) ? "0" : timestamp);
        return captured;
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (offset.getPosition() != null) {
            refreshSnapshot(String.valueOf(offset.getPosition()));
        }
    }

    private void initTableMap() {
        tableMap.clear();
        if (CollectionUtils.isEmpty(sourceTable)) {
            return;
        }
        for (Table table : sourceTable) {
            if (filterTable.contains(table.getName())) {
                tableMap.put(table.getName(), table);
            }
        }
    }

    private void runLogProxy() throws Exception {
        DatabaseConfig config = getConnectorInstance().getConfig();
        OceanBaseCdcConfig cdcConfig = new OceanBaseCdcConfig(config);
        Assert.hasText(cdcConfig.getLogProxyHost(), "LogProxy 主机不能为空");
        Assert.isTrue(cdcConfig.getLogProxyPort() > 0, "LogProxy 端口无效");

        ObReaderConfig obReaderConfig = buildObReaderConfig(config, cdcConfig);
        if (StringUtil.isNotBlank(safeTimestamp)) {
            obReaderConfig.updateCheckpoint(safeTimestamp);
        }

        ClientConf clientConf = ClientConf.builder()
                .transferQueueSize(1000)
                .connectTimeoutMs(60000)
                .ignoreUnknownRecordType(true)
                .build();

        client = new LogProxyClient(cdcConfig.getLogProxyHost(), cdcConfig.getLogProxyPort(), obReaderConfig, clientConf);
        client.addListener(new InnerRecordListener());
        client.start();
        client.join();
    }

    private ObReaderConfig buildObReaderConfig(DatabaseConfig config, OceanBaseCdcConfig cdcConfig) throws Exception {
        ObReaderConfig obReaderConfig = new ObReaderConfig();
        String rsList = cdcConfig.getRsList();
        if (StringUtil.isBlank(rsList)) {
            rsList = queryRsList(config);
        }
        if (StringUtil.isNotBlank(rsList)) {
            obReaderConfig.setRsList(rsList);
        }
        if (StringUtil.isNotBlank(cdcConfig.getConfigUrl())) {
            obReaderConfig.setClusterUrl(cdcConfig.getConfigUrl());
        }
        obReaderConfig.setUsername(cdcConfig.resolveUsername());
        obReaderConfig.setPassword(cdcConfig.getPassword());
        if (StringUtil.isNotBlank(cdcConfig.getSysUsername())) {
            obReaderConfig.setSysUsername(cdcConfig.getSysUsername());
        }
        if (StringUtil.isNotBlank(cdcConfig.getSysPassword())) {
            obReaderConfig.setSysPassword(cdcConfig.getSysPassword());
        }
        obReaderConfig.setWorkingMode(cdcConfig.getWorkingMode());
        obReaderConfig.setTimezone(cdcConfig.getTimezone());
        obReaderConfig.setTableWhiteList(buildTableWhiteList(cdcConfig));
        obReaderConfig.setStartTimestamp(parseStartTimestamp());
        return obReaderConfig;
    }

    private long parseStartTimestamp() {
        String timestamp = snapshot.get(OceanBaseCdcConstants.SNAPSHOT_TIMESTAMP);
        if (StringUtil.isBlank(timestamp)) {
            return 0L;
        }
        try {
            return Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            logger.warn("非法增量位点 timestamp={}, 将从当前位点开始", timestamp);
            return 0L;
        }
    }

    private String buildTableWhiteList(OceanBaseCdcConfig cdcConfig) {
        Assert.hasText(cdcConfig.getTenantName(), "租户名称不能为空");
        Assert.hasText(database, "数据库名不能为空");
        if (CollectionUtils.isEmpty(filterTable)) {
            return cdcConfig.getTenantName() + "." + database + ".*";
        }
        return filterTable.stream()
                .map(table -> cdcConfig.getTenantName() + "." + database + "." + table)
                .collect(Collectors.joining(";"));
    }

    private String queryRsList(DatabaseConfig config) {
        try {
            return getConnectorInstance().execute(databaseTemplate -> {
                List<Map<String, Object>> rows = databaseTemplate.queryForList("SHOW PARAMETERS LIKE 'rootservice_list'");
                if (CollectionUtils.isEmpty(rows)) {
                    return null;
                }
                Map<String, Object> row = rows.get(0);
                Object value = row.get("VALUE");
                if (value == null) {
                    value = row.get("value");
                }
                return value == null ? null : value.toString();
            });
        } catch (Exception e) {
            logger.warn("自动查询 rootservice_list 失败: {}", e.getMessage());
            return null;
        }
    }

    private void refreshSnapshot(String timestamp) {
        snapshot.put(OceanBaseCdcConstants.SNAPSHOT_TIMESTAMP, timestamp);
        safeTimestamp = timestamp;
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            while (running) {
                try {
                    sendChangedEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    TimeUnit.MILLISECONDS.sleep(1);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void flushBuffer() {
        List<LogMessage> messages = new ArrayList<>(logMessageBuffer);
        logMessageBuffer.clear();
        for (LogMessage message : messages) {
            processLogMessage(message);
        }
    }

    private void processLogMessage(LogMessage message) {
        DataMessage.Record.Type type = message.getOpt();
        if (type == null) {
            return;
        }
        String dbName = OceanBaseLogMessageParser.parseDatabaseName(message.getDbName());
        String tableName = message.getTableName();
        if (!isFilterTable(dbName, tableName)) {
            return;
        }

        Table table = tableMap.get(tableName);
        if (table == null) {
            logger.warn("未找到表 {} 的列元数据，跳过事件 {}", tableName, type);
            return;
        }

        String offset = message.getSafeTimestamp();
        safeTimestamp = offset;
        refreshSnapshot(offset);

        switch (type) {
            case INSERT:
                dispatchRowEvent(tableName, ConnectorConstant.OPERTION_INSERT,
                        OceanBaseLogMessageParser.toRowList(table.getColumn(), OceanBaseLogMessageParser.toValueMap(message, false)), offset);
                break;
            case UPDATE:
                dispatchRowEvent(tableName, ConnectorConstant.OPERTION_UPDATE,
                        OceanBaseLogMessageParser.toRowList(table.getColumn(), OceanBaseLogMessageParser.toValueMap(message, false)), offset);
                break;
            case DELETE:
                dispatchRowEvent(tableName, ConnectorConstant.OPERTION_DELETE,
                        OceanBaseLogMessageParser.toRowList(table.getColumn(), OceanBaseLogMessageParser.toValueMap(message, true)), offset);
                break;
            case DDL:
                String ddl = OceanBaseLogMessageParser.readDdlSql(message);
                if (StringUtil.isNotBlank(ddl)) {
                    trySendEvent(new DDLChangedEvent(tableName, ConnectorConstant.OPERTION_ALTER, ddl,
                            OceanBaseCdcConstants.OFFSET_LABEL, offset));
                }
                break;
            default:
                break;
        }
    }

    private void dispatchRowEvent(String tableName, String operation, List<Object> rowData, String offset) {
        if (CollectionUtils.isEmpty(rowData)) {
            return;
        }
        trySendEvent(new RowChangedEvent(tableName, operation, rowData, OceanBaseCdcConstants.OFFSET_LABEL, offset));
    }

    private boolean isFilterTable(String dbName, String tableName) {
        return StringUtil.equalsIgnoreCase(database, dbName) && filterTable.contains(tableName);
    }

    final class InnerRecordListener implements RecordListener {

        @Override
        public void notify(LogMessage message) {
            if (!running) {
                if (client != null) {
                    client.stop();
                }
                return;
            }
            if (message == null || message.getOpt() == null) {
                return;
            }
            switch (message.getOpt()) {
                case HEARTBEAT:
                case BEGIN:
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                    logMessageBuffer.add(message);
                    break;
                case COMMIT:
                    flushBuffer();
                    break;
                case DDL:
                    logMessageBuffer.add(message);
                    flushBuffer();
                    break;
                default:
                    logger.debug("忽略 LogProxy 事件类型: {}", message.getOpt());
                    break;
            }
        }

        @Override
        public void onException(LogProxyClientException e) {
            logger.error("LogProxyClient 异常: {}", e.getMessage(), e);
            if (running) {
                errorEvent(new OceanBaseException(e));
            }
            if (client != null) {
                client.stop();
            }
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            try {
                runLogProxy();
            } catch (Exception e) {
                if (running) {
                    logger.error("OceanBase LogProxy 监听线程异常: {}", e.getMessage(), e);
                    errorEvent(new OceanBaseException(e));
                }
            } finally {
                boolean hasTimestamp = snapshot.containsKey(OceanBaseCdcConstants.SNAPSHOT_TIMESTAMP);
                if (!hasTimestamp && StringUtil.isNotBlank(safeTimestamp)) {
                    refreshSnapshot(safeTimestamp);
                }
                if (hasTimestamp || StringUtil.isNotBlank(safeTimestamp)) {
                    forceFlushEvent();
                }
            }
        }
    }
}
