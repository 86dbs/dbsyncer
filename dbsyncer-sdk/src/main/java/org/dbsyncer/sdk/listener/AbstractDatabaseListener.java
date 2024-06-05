/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 21:46
 */
public abstract class AbstractDatabaseListener extends AbstractListener<DatabaseConnectorInstance> {

    /**
     * 自定义SQL，支持1对多
     * <p>MY_USER > [用户表1, 用户表2]
     */
    private Map<String, List<DqlMapper>> dqlMap = new ConcurrentHashMap<>();

    /**
     * 发送增量事件
     *
     * @param event
     */
    protected void sendChangedEvent(ChangedEvent event) {
        changeEvent(event);
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

        RowChangedEvent changedEvent = (RowChangedEvent) event;
        boolean processed = false;
        for (DqlMapper dqlMapper : dqlMappers) {
            if (!processed) {
                switch (event.getEvent()) {
                    case ConnectorConstant.OPERTION_UPDATE:
                    case ConnectorConstant.OPERTION_INSERT:
                        try {
                            queryDqlData(dqlMapper, changedEvent.getDataList());
                        } catch (Exception e) {
                            return;
                        }
                        break;
                    default:
                        break;
                }
                processed = true;
            }
            changedEvent.setSourceTableName(dqlMapper.sqlName);
            changeEvent(changedEvent);
        }
    }

    /**
     * 初始化Dql连接配置
     */
    protected void postProcessDqlBeforeInitialization() {
        DatabaseConnectorInstance instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDatabaseConnector service = (AbstractDatabaseConnector) connectorService;
        String quotation = service.buildSqlWithQuotation();

        // <用户表, MY_USER>
        Map<String, String> tableMap = new HashMap<>();
        instance.getConfig().getSqlTables().forEach(s -> tableMap.put(s.getSqlName(), s.getTable()));
        // 清空默认表名
        filterTable.clear();
        for (Table t : sourceTable) {
            String sql = t.getSql();
            String sqlName = t.getName();
            List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(t);
            String tableName = tableMap.get(sqlName);
            Assert.hasText(sql, "The sql is null.");
            Assert.hasText(tableName, "The tableName is null.");

            MetaInfo metaInfo = service.getMetaInfo(instance, sqlName);
            final List<Field> column = metaInfo.getColumn();
            Assert.notEmpty(column, String.format("The column of table name '%s' is empty.", sqlName));

            sql = sql.replace("\t", " ");
            sql = sql.replace("\r", " ");
            sql = sql.replace("\n", " ");

            StringBuilder querySql = new StringBuilder(sql);
            String temp = sql.toUpperCase();
            boolean notContainsWhere = !StringUtil.contains(temp, " WHERE ");
            querySql.append(notContainsWhere ? " WHERE " : StringUtil.EMPTY);
            PrimaryKeyUtil.buildSql(querySql, primaryKeys, quotation, " AND ", " = ? ", notContainsWhere);
            DqlMapper dqlMapper = new DqlMapper(instance, sqlName, querySql.toString(), column, getPrimaryKeyIndexArray(column, primaryKeys));
            if (!dqlMap.containsKey(tableName)) {
                dqlMap.putIfAbsent(tableName, new ArrayList<>());
            }
            dqlMap.get(tableName).add(dqlMapper);
            // 注册监听表名
            filterTable.add(tableName);
        }
    }

    /**
     * 获取主表主键索引
     *
     * @param column
     * @param primaryKeys
     * @return
     */
    protected Integer[] getPrimaryKeyIndexArray(List<Field> column, List<String> primaryKeys) {
        List<Integer> indexList = new ArrayList<>();
        for (Field f : column) {
            if (primaryKeys.contains(f.getName())) {
                indexList.add(column.indexOf(f));
            }
        }
        Assert.isTrue(!CollectionUtils.isEmpty(indexList), "The primaryKeys is invalid.");
        Object[] indexArray = indexList.toArray();
        Integer[] newIndexArray = new Integer[indexArray.length];
        System.arraycopy(indexArray, 0, newIndexArray, 0, indexArray.length);
        return newIndexArray;
    }

    private void queryDqlData(DqlMapper dqlMapper, List<Object> data) {
        if (!CollectionUtils.isEmpty(data)) {
            Map<String, Object> row = dqlMapper.instance.execute(databaseTemplate -> {
                int size = dqlMapper.primaryKeyIndexArray.length;
                Object[] args = new Object[size];
                for (int i = 0; i < size; i++) {
                    args[i] = data.get(dqlMapper.primaryKeyIndexArray[i]);
                }
                return databaseTemplate.queryForMap(dqlMapper.sql, args);
            });
            if (!CollectionUtils.isEmpty(row)) {
                data.clear();
                dqlMapper.column.forEach(field -> data.add(row.get(field.getName())));
            }
        }
    }

    final class DqlMapper {
        DatabaseConnectorInstance instance;
        String sqlName;
        String sql;
        List<Field> column;
        Integer[] primaryKeyIndexArray;

        public DqlMapper(DatabaseConnectorInstance instance, String sqlName, String sql, List<Field> column, Integer[] primaryKeyIndexArray) {
            this.instance = instance;
            this.sqlName = sqlName;
            this.sql = sql;
            this.column = column;
            this.primaryKeyIndexArray = primaryKeyIndexArray;
        }
    }

}