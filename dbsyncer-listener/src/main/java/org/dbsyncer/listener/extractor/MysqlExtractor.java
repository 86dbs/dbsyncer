package org.dbsyncer.listener.extractor;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.DefaultExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.config.Host;
import org.dbsyncer.listener.mysql.binlog.BinlogEventListener;
import org.dbsyncer.listener.mysql.binlog.BinlogEventV4;
import org.dbsyncer.listener.mysql.binlog.BinlogRemoteClient;
import org.dbsyncer.listener.mysql.binlog.impl.event.*;
import org.dbsyncer.listener.mysql.common.glossary.Column;
import org.dbsyncer.listener.mysql.common.glossary.Pair;
import org.dbsyncer.listener.mysql.common.glossary.Row;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class MysqlExtractor extends DefaultExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String BINLOG_FILENAME = "fileName";
    private static final String BINLOG_POSITION = "position";
    private BinlogRemoteClient client;
    private List<Host> cluster;
    private int master = 0;

    @Override
    public void extract() {
        try {
            final DatabaseConfig config = (DatabaseConfig) connectorConfig;
            cluster = readNodes(config.getUrl());
            Assert.notEmpty(cluster, "Mysql连接地址有误.");

            final Host host = cluster.get(master);
            final String username = config.getUsername();
            final String password = config.getPassword();
            final String threadSuffixName = "mysql-binlog";

            client = new BinlogRemoteClient(host.getIp(), host.getPort(), username, password, threadSuffixName);
            client.setBinlogFileName(map.get(BINLOG_FILENAME));
            String pos = map.get(BINLOG_POSITION);
            client.setBinlogPosition(StringUtils.isBlank(pos) ? 0 : Long.parseLong(pos));
            client.setBinlogEventListener(new MysqlEventListener());
            client.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        }
    }

    @Override
    public void extractTiming() {

    }

    @Override
    public void close() {
        try {
            client.stopQuietly();
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }
    }

    private List<Host> readNodes(String url) {
        if (StringUtils.isBlank(url)) {
            return Collections.EMPTY_LIST;
        }
        Matcher matcher = compile("(//)(?!(/)).+?(/)").matcher(url);
        while (matcher.find()) {
            url = matcher.group(0);
            break;
        }
        url = StringUtils.replace(url, "/", "");

        List<Host> cluster = new ArrayList<>();
        String[] arr = StringUtils.split(url, ",");
        int size = arr.length;
        for (int i = 0; i < size; i++) {
            String[] host = StringUtils.split(arr[i], ":");
            if (2 == host.length) {
                cluster.add(new Host(host[0], Integer.parseInt(host[1])));
            }
        }
        return cluster;
    }

    /**
     * 有变化触发刷新binlog增量事件
     *
     * @param event
     */
    private void refresh(AbstractBinlogEventV4 event) {
        String binlogFilename = event.getBinlogFilename();
        long nextPosition = event.getHeader().getNextPosition();
        if (!StringUtils.equals(binlogFilename, client.getBinlogFileName()) || 0 != Long.compare(nextPosition,
                client.getBinlogPosition())) {
            client.setBinlogFileName(binlogFilename);
            client.setBinlogPosition(nextPosition);
            map.put(BINLOG_FILENAME, client.getBinlogFileName());
            map.put(BINLOG_POSITION, String.valueOf(nextPosition));
            flushEvent();
        }
    }

    final class MysqlEventListener implements BinlogEventListener {

        private Map<Long, String> table = new HashMap<>();

        @Override
        public void onEvents(BinlogEventV4 event) {
            if (event == null) {
                logger.error("binlog event is null");
                return;
            }

            if (event instanceof TableMapEvent) {
                TableMapEvent tableEvent = (TableMapEvent) event;
                table.putIfAbsent(tableEvent.getTableId(), tableEvent.getTableName().toString());
                return;
            }

            if (event instanceof UpdateRowsEventV2) {
                UpdateRowsEventV2 e = (UpdateRowsEventV2) event;
                final String tableName = table.get(e.getTableId());
                List<Pair<Row>> rows = e.getRows();
                for (Pair<Row> p : rows) {
                    List<Object> before = new ArrayList<>();
                    List<Object> after = new ArrayList<>();
                    addAll(before, p.getBefore().getColumns());
                    addAll(after, p.getAfter().getColumns());
                    changedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, before, after);
                    break;
                }
                return;
            }

            if (event instanceof WriteRowsEventV2) {
                WriteRowsEventV2 e = (WriteRowsEventV2) event;
                final String tableName = table.get(e.getTableId());
                List<Row> rows = e.getRows();
                for (Row row : rows) {
                    List<Object> after = new ArrayList<>();
                    addAll(after, row.getColumns());
                    changedEvent(tableName, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, after);
                    break;
                }
                return;
            }

            if (event instanceof DeleteRowsEventV2) {
                DeleteRowsEventV2 e = (DeleteRowsEventV2) event;
                final String tableName = table.get(e.getTableId());
                List<Row> rows = e.getRows();
                for (Row row : rows) {
                    List<Object> before = new ArrayList<>();
                    addAll(before, row.getColumns());
                    changedEvent(tableName, ConnectorConstant.OPERTION_DELETE, before, Collections.EMPTY_LIST);
                    break;
                }
                return;
            }

            // 处理事件优先级：RotateEvent > FormatDescriptionEvent > TableMapEvent > RowsEvent > XidEvent
            if (event instanceof XidEvent) {
                refresh((XidEvent) event);
                return;
            }

            // 切换binlog
            if (event instanceof RotateEvent) {
                refresh((RotateEvent) event);
                return;
            }

        }

        private void addAll(List<Object> before, List<Column> columns) {
            columns.forEach(c -> before.add((c instanceof StringColumn) ? c.toString() : c.getValue()));
        }

    }

}