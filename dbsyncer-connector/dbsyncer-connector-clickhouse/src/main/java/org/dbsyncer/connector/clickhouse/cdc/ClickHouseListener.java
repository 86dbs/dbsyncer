/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.cdc;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.clickhouse.ClickHouseException;
import org.dbsyncer.connector.clickhouse.constant.ClickHouseConfigConstant;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse 增量监听器：基于主键游标轮询（适用于 MergeTree 等追加写入场景）。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseListener extends AbstractDatabaseListener {

    private static final String SNAPSHOT_PREFIX = "cursor_";
    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 5;
    private static final int DEFAULT_BATCH_SIZE = 500;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile boolean running;

    @Override
    public void start() {
        running = true;
        int interval = resolvePollIntervalSeconds();
        scheduledTaskService.start(interval * 1000L, this::poll);
        logger.info("ClickHouseListener started, poll interval={}s, batchSize={}", interval, resolveBatchSize());
    }

    @Override
    public void close() {
        running = false;
    }

    private void poll() {
        if (!running || CollectionUtils.isEmpty(sourceTable)) {
            return;
        }
        Database database = (Database) connectorService;
        DatabaseConnectorInstance instance = getConnectorInstance();
        for (Table table : sourceTable) {
            if (!CollectionUtils.isEmpty(filterTable) && !filterTable.contains(table.getName())) {
                continue;
            }
            try {
                pollTable(database, instance, table);
            } catch (Exception e) {
                logger.error("ClickHouse 增量轮询失败, table={}", table.getName(), e);
                errorEvent(e);
            }
        }
    }

    private void pollTable(Database database, DatabaseConnectorInstance instance, Table table) {
        List<Field> columns = table.getColumn();
        if (CollectionUtils.isEmpty(columns)) {
            return;
        }
        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(columns);
        if (CollectionUtils.isEmpty(pkFields) || pkFields.size() != 1) {
            throw new ClickHouseException(String.format("ClickHouse 日志增量要求单字段数值主键, 表=%s", table.getName()));
        }
        Field pkField = pkFields.get(0);
        if (!PrimaryKeyUtil.isSupportedCursor(columns)) {
            throw new ClickHouseException(String.format("ClickHouse 日志增量主键类型不支持游标, 表=%s, 字段=%s", table.getName(), pkField.getName()));
        }

        String snapshotKey = SNAPSHOT_PREFIX + table.getName();
        Object lastCursor = parseCursor(snapshot.get(snapshotKey));
        String qualifiedTable = database.buildWithQuotation(table.getName());
        String pkName = database.buildWithQuotation(pkField.getName());
        String columnSql = buildColumnSql(database, columns);
        int batchSize = resolveBatchSize();

        StringBuilder sql = new StringBuilder("SELECT ").append(columnSql).append(" FROM ").append(qualifiedTable);
        List<Object> args = new ArrayList<>();
        if (lastCursor != null) {
            sql.append(" WHERE ").append(pkName).append(" > ?");
            args.add(lastCursor);
        }
        sql.append(" ORDER BY ").append(pkName).append(" ASC LIMIT ?");
        args.add(batchSize);

        List<Map<String, Object>> rows = instance.execute(databaseTemplate ->
                databaseTemplate.queryForList(sql.toString(), args.toArray()));
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }

        Object maxCursor = lastCursor;
        for (Map<String, Object> row : rows) {
            List<Object> rowData = new ArrayList<>(columns.size());
            for (Field column : columns) {
                rowData.add(row.get(column.getName()));
            }
            Object currentPk = row.get(pkField.getName());
            if (currentPk != null) {
                maxCursor = currentPk;
            }
            sendChangedEvent(new RowChangedEvent(table.getName(), ConnectorConstant.OPERTION_INSERT, rowData, null, null));
        }
        if (maxCursor != null && (lastCursor == null || compareCursor(maxCursor, lastCursor) > 0)) {
            snapshot.put(snapshotKey, String.valueOf(maxCursor));
            forceFlushEvent();
        }
    }

    private String buildColumnSql(Database database, List<Field> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (Field column : columns) {
            names.add(database.buildWithQuotation(column.getName()));
        }
        return StringUtil.join(names, StringUtil.COMMA);
    }

    private Object parseCursor(String cursor) {
        if (StringUtil.isBlank(cursor)) {
            return null;
        }
        if (NumberUtil.isCreatable(cursor)) {
            return cursor.contains(".") ? Double.valueOf(cursor) : NumberUtil.toLong(cursor, 0L);
        }
        return cursor;
    }

    private int compareCursor(Object current, Object last) {
        if (current instanceof Number && last instanceof Number) {
            return Double.compare(((Number) current).doubleValue(), ((Number) last).doubleValue());
        }
        return String.valueOf(current).compareTo(String.valueOf(last));
    }

    private int resolvePollIntervalSeconds() {
        if (connectorConfig == null || connectorConfig.getExtInfo() == null) {
            return DEFAULT_POLL_INTERVAL_SECONDS;
        }
        return NumberUtil.toInt(connectorConfig.getExtInfo().getProperty(ClickHouseConfigConstant.POLL_INTERVAL_SECONDS),
                DEFAULT_POLL_INTERVAL_SECONDS);
    }

    private int resolveBatchSize() {
        if (connectorConfig == null || connectorConfig.getExtInfo() == null) {
            return DEFAULT_BATCH_SIZE;
        }
        return NumberUtil.toInt(connectorConfig.getExtInfo().getProperty(ClickHouseConfigConstant.POLL_BATCH_SIZE),
                DEFAULT_BATCH_SIZE);
    }
}
