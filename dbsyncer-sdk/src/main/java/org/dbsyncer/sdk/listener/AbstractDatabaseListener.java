/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 21:46
 */
public abstract class AbstractDatabaseListener extends AbstractListener<DatabaseConnectorInstance> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 自定义SQL，支持1对多
     * <p>MY_USER > [用户表1, 用户表2]
     */
    private final Map<String, List<DqlMapper>> dqlMap = new ConcurrentHashMap<>();


    /**
     * 统一的事件发送方法，带队列满重试机制
     * 当队列满时，会根据配置的重试间隔进行阻塞重试，确保数据不丢失
     *
     * @param event 变更事件
     */
    protected void trySendEvent(ChangedEvent event) {
        // 检查连接状态（子类可以重写 isConnected() 方法来自定义连接检查逻辑）
        while (true) {
            try {
                changeEvent(event);
                return;  // 发送成功
            } catch (QueueOverflowException e) {
                // 队列已满，记录警告日志
                long retryInterval = listenerConfig.getQueueOverflowRetryInterval();
                logger.warn("队列已满，等待{}毫秒后重试，table: {}, event: {}, error: {}",
                        retryInterval, event.getSourceTableName(), event.getEvent(), e.getMessage());
                try {
                    TimeUnit.MILLISECONDS.sleep(retryInterval);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    logger.warn("重试等待被中断");
                    break;
                }
            } catch (Exception e) {
                // 发送事件时发生非队列溢出异常，记录详细错误信息并通过errorEvent传递
                String errorMsg = String.format("发送事件失败，table: %s, event: %s, error: %s",
                        event.getSourceTableName(), event.getEvent(), e.getMessage());
                logger.error(errorMsg, e);
                errorEvent(e);
                // 不再重试，避免无限循环
                break;
            }
        }
    }

    /**
     * 发送DQL增量事件
     *
     * @param event
     */
    protected void sendDqlChangedEvent(ChangedEvent event) {
        if (null == event) {
            return;
        }
        List<DqlMapper> dqlMappers = dqlMap.get(event.getSourceTableName());
        if (CollectionUtils.isEmpty(dqlMappers)) {
            return;
        }

        boolean processed = false;
        for (DqlMapper dqlMapper : dqlMappers) {
            if (!processed) {
                switch (event.getEvent()) {
                    case ConnectorConstant.OPERTION_UPDATE:
                    case ConnectorConstant.OPERTION_INSERT:
                        try {
                            queryDqlData(dqlMapper, event.getChangedRow());
                        } catch (Exception e) {
                            return;
                        }
                        break;
                    case ConnectorConstant.OPERTION_DELETE:
                        getPKData(dqlMapper, event.getChangedRow());
                        break;
                    default:
                        break;
                }
                processed = true;
            }
            event.setSourceTableName(dqlMapper.sqlName);
            changeEvent(event);
        }
    }

    /**
     * 初始化Dql连接配置
     */
    protected void postProcessDqlBeforeInitialization() throws Exception {
        DatabaseConnectorInstance instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;

        // <用户表, MY_USER>
        Map<String, String> tableMap = new HashMap<>();
        instance.getConfig().getSqlTables().forEach(s -> tableMap.put(s.getSqlName(), s.getTable()));
        // 清空默认表名
        filterTable.clear();
        for (Table t : sourceTable) {
            String sql = t.getSql();
            String sqlName = t.getName();
            String tableName = tableMap.get(sqlName);
            Assert.hasText(sql, "The sql is null.");
            Assert.hasText(tableName, "The tableName is null.");

            MetaInfo tableMetaInfo = service.getMetaInfo(instance, tableName);
            List<Field> tableColumns = tableMetaInfo.getColumn();
            Assert.notEmpty(tableColumns, String.format("The column of table name '%s' is empty.", tableName));
            List<Field> primaryFields = PrimaryKeyUtil.findPrimaryKeyFields(tableColumns);
            Assert.notEmpty(primaryFields, String.format("主表 %s 缺少主键.", tableName));
            List<String> primaryKeys = primaryFields.stream().map(Field::getName).collect(Collectors.toList());
            Map<String, Integer> tablePKIndexMap = new HashMap<>(primaryKeys.size());
            List<Integer> tablePKIndex = getPKIndex(tableColumns, tablePKIndexMap);

            MetaInfo sqlMetaInfo = service.getMetaInfo(instance, sqlName);
            final List<Field> sqlColumns = sqlMetaInfo.getColumn();
            Assert.notEmpty(sqlColumns, String.format("The column of table name '%s' is empty.", sqlName));
            Map<Integer, Integer> sqlPKIndexMap = getPKIndexMap(sqlColumns, tablePKIndexMap);
            Assert.notEmpty(sqlPKIndexMap, String.format("表 %s 缺少主键.", sqlName));

            // 使用SQL模板构建DQL查询
            String querySql = service.sqlTemplate.buildDqlQuerySql(sql, primaryKeys);
            DqlMapper dqlMapper = new DqlMapper(instance, sqlName, querySql, sqlColumns, tablePKIndex, sqlPKIndexMap);
            dqlMap.compute(tableName, (k, v) -> {
                if (v == null) {
                    return new ArrayList<>();
                }
                return v;
            }).add(dqlMapper);

            // 注册监听表名
            filterTable.add(tableName);
        }
    }

    private Map<Integer, Integer> getPKIndexMap(List<Field> column, Map<String, Integer> tablePKIndexMap) {
        Map<String, Integer> lowerCasePKMap = new HashMap<>();
        tablePKIndexMap.forEach((k, v) -> lowerCasePKMap.put(k.toLowerCase(), v));
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < column.size(); i++) {
            final int index = i;
            String colNameLower = column.get(i).getName().toLowerCase();
            lowerCasePKMap.computeIfPresent(colNameLower, (k, v) -> map.put(index, v));
        }
        return map;
    }

    private List<Integer> getPKIndex(List<Field> column, Map<String, Integer> tablePKIndexMap) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < column.size(); i++) {
            if (column.get(i).isPk()) {
                list.add(i);
                tablePKIndexMap.put(column.get(i).getName(), i);
            }
        }
        return list;
    }

    private void queryDqlData(DqlMapper dqlMapper, List<Object> data) throws Exception {
        if (!CollectionUtils.isEmpty(data)) {
            Map<String, Object> row = dqlMapper.instance.execute(databaseTemplate -> {
                int size = dqlMapper.tablePKIndex.size();
                Object[] args = new Object[size];
                for (int i = 0; i < size; i++) {
                    args[i] = data.get(dqlMapper.tablePKIndex.get(i));
                }
                return databaseTemplate.queryForMap(dqlMapper.sql, args);
            });
            if (!CollectionUtils.isEmpty(row)) {
                data.clear();
                dqlMapper.column.forEach(field -> data.add(row.get(field.getName())));
            }
        }
    }

    private void getPKData(DqlMapper dqlMapper, List<Object> data) {
        if (!CollectionUtils.isEmpty(data)) {
            int size = dqlMapper.column.size();
            List<Object> row = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                if (dqlMapper.sqlPKIndexMap.containsKey(i)) {
                    row.add(data.get(dqlMapper.sqlPKIndexMap.get(i)));
                    continue;
                }
                row.add(null);
            }
            if (!CollectionUtils.isEmpty(row)) {
                data.clear();
                data.addAll(row);
            }
        }
    }

    static final class DqlMapper {
        DatabaseConnectorInstance instance;
        String sqlName;
        String sql;
        List<Field> column;
        List<Integer> tablePKIndex;
        Map<Integer, Integer> sqlPKIndexMap;

        public DqlMapper(DatabaseConnectorInstance instance, String sqlName, String sql, List<Field> column, List<Integer> tablePKIndex, Map<Integer, Integer> sqlPKIndexMap) {
            this.instance = instance;
            this.sqlName = sqlName;
            this.sql = sql;
            this.column = column;
            this.tablePKIndex = tablePKIndex;
            this.sqlPKIndexMap = sqlPKIndexMap;
        }
    }

}