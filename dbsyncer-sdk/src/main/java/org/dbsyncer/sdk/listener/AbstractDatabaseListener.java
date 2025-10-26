/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.SqlTable;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private final Map<String, List<DqlMapper>> dqlMap = new ConcurrentHashMap<>();

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
    protected void postProcessDqlBeforeInitialization() {
        if (CollectionUtils.isEmpty(sqlTables)) {
            return;
        }
        // <用户表, MY_USER>
        Map<String, SqlTable> sqlTableMap = new HashMap<>();
        sqlTables.forEach(s -> sqlTableMap.put(s.getSqlName(), s));

        DatabaseConnectorInstance instance = (DatabaseConnectorInstance) connectorInstance;
        AbstractDQLConnector service = (AbstractDQLConnector) connectorService;
        String quotation = service.buildSqlWithQuotation();

        // 清空默认表名
        filterTable.clear();
        for (Table t : sourceTable) {
            String sql = t.getSql();
            String sqlName = t.getName();
            SqlTable sqlTable = sqlTableMap.get(sqlName);
            if (sqlTable == null) {
                continue;
            }
            String tableName = sqlTable.getTable();
            Assert.hasText(sql, "The sql is null.");
            Assert.hasText(tableName, "The tableName is null.");

            MetaInfo tableMetaInfo = getMetaInfo(service, instance, sqlTable.getTable());
            Assert.notNull(tableMetaInfo, String.format("The table %s is invalid.", sqlTable.getTable()));
            List<Field> tableColumns = tableMetaInfo.getColumn();
            Assert.notEmpty(tableColumns, String.format("The column of table name '%s' is empty.", tableName));
            List<Field> primaryFields = PrimaryKeyUtil.findPrimaryKeyFields(tableColumns);
            Assert.notEmpty(primaryFields, String.format("主表 %s 缺少主键.", tableName));
            List<String> primaryKeys = primaryFields.stream().map(Field::getName).collect(Collectors.toList());
            Map<String, Integer> tablePKIndexMap = new HashMap<>(primaryKeys.size());
            List<Integer> tablePKIndex = getPKIndex(tableColumns, tablePKIndexMap);

            MetaInfo sqlMetaInfo = getMetaInfo(service, instance, sqlTable);
            Assert.notNull(sqlMetaInfo, "The sql table is not exist.");
            final List<Field> sqlColumns = sqlMetaInfo.getColumn();
            Assert.notEmpty(sqlColumns, String.format("The column of table name '%s' is empty.", sqlName));
            Map<Integer, Integer> sqlPKIndexMap = getPKIndexMap(sqlColumns, tablePKIndexMap);
            Assert.notEmpty(sqlPKIndexMap, String.format("表 %s 缺少主键.", sqlName));

            sql = sql.replace("\t", " ");
            sql = sql.replace("\r", " ");
            sql = sql.replace("\n", " ");

            StringBuilder querySql = new StringBuilder(sql);
            String temp = sql.toUpperCase();
            boolean notContainsWhere = !StringUtil.contains(temp, " WHERE ");
            querySql.append(notContainsWhere ? " WHERE " : StringUtil.EMPTY);
            PrimaryKeyUtil.buildSql(querySql, primaryKeys, quotation, " AND ", " = ? ", notContainsWhere);
            DqlMapper dqlMapper = new DqlMapper(instance, sqlName, querySql.toString(), sqlColumns, tablePKIndex, sqlPKIndexMap);
            dqlMap.compute(tableName, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(dqlMapper);
                return v;
            });

            // 注册监听表名
            filterTable.add(tableName);
        }
    }

    private MetaInfo getMetaInfo(AbstractDQLConnector service, DatabaseConnectorInstance instance, String tablePattern) {
        ConnectorServiceContext context = new DefaultConnectorServiceContext(database, schema, tablePattern);
        return getFirstMetaInfo(service.getTableMetaInfo(instance, context));
    }

    private MetaInfo getMetaInfo(AbstractDQLConnector service, DatabaseConnectorInstance instance, SqlTable sqlTable) {
        ConnectorServiceContext context = new DefaultConnectorServiceContext(database, schema, sqlTable);
        return getFirstMetaInfo(service.getMetaInfo(instance, context));
    }

    private MetaInfo getFirstMetaInfo(List<MetaInfo> metaInfos){
        return CollectionUtils.isEmpty(metaInfos) ? null : metaInfos.get(0);
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

    private void queryDqlData(DqlMapper dqlMapper, List<Object> data) {
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