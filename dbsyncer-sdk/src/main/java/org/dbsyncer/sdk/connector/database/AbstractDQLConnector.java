/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.SqlTable;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/6 22:16
 */
public abstract class AbstractDQLConnector extends AbstractDatabaseConnector {

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance) {
        DatabaseConfig cfg = connectorInstance.getConfig();
        List<SqlTable> sqlTables = cfg.getSqlTables();
        List<Table> tables = new ArrayList<>();
        if (!CollectionUtils.isEmpty(sqlTables)) {
            sqlTables.forEach(s ->
                tables.add(new Table(s.getSqlName(), TableTypeEnum.TABLE.getCode(), Collections.EMPTY_LIST, s.getSql(), null))
            );
        }
        return tables;
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorInstance connectorInstance, String sqlName) {
        DatabaseConfig cfg = connectorInstance.getConfig();
        List<SqlTable> sqlTables = cfg.getSqlTables();
        for (SqlTable s : sqlTables) {
            if (!StringUtil.equals(s.getSqlName(), sqlName)) {
                continue;
            }
            String sql = s.getSql().toUpperCase();
            sql = sql.replace("\t", " ");
            sql = sql.replace("\r", " ");
            sql = sql.replace("\n", " ");
            String metaSql = StringUtil.contains(sql, " WHERE ") ? s.getSql() + " AND 1!=1 " : s.getSql() + " WHERE 1!=1 ";
            String tableName = s.getTable();
            return connectorInstance.execute(databaseTemplate -> {
                SqlRowSet sqlRowSet = databaseTemplate.queryForRowSet(metaSql);
                ResultSetWrappingSqlRowSet rowSet = (ResultSetWrappingSqlRowSet) sqlRowSet;
                SqlRowSetMetaData metaData = rowSet.getMetaData();

                // 查询表字段信息
                int columnCount = metaData.getColumnCount();
                if (1 > columnCount) {
                    throw new SdkException("查询表字段不能为空.");
                }
                List<Field> fields = new ArrayList<>(columnCount);
                Map<String, List<String>> tables = new HashMap<>();
                try {
                    SimpleConnection connection = databaseTemplate.getSimpleConnection();
                    DatabaseMetaData md = connection.getMetaData();
                    Connection conn = connection.getConnection();
                    DatabaseConfig config = connectorInstance.getConfig();
                    final String catalog = getCatalog(config, conn);
                    final String schema = getSchema(config, conn);
                    String name = null;
                    String label = null;
                    String typeName = null;
                    String table = null;
                    int columnType;
                    boolean pk;
                    for (int i = 1; i <= columnCount; i++) {
                        table = StringUtil.isNotBlank(tableName) ? tableName : metaData.getTableName(i);
                        if (null == tables.get(table)) {
                            tables.putIfAbsent(table, findTablePrimaryKeys(md, catalog, schema, table));
                        }
                        name = metaData.getColumnName(i);
                        label = metaData.getColumnLabel(i);
                        typeName = metaData.getColumnTypeName(i);
                        columnType = metaData.getColumnType(i);
                        pk = isPk(tables, table, name);
                        fields.add(new Field(label, typeName, columnType, pk));
                    }
                } finally {
                    tables.clear();
                }
                return new MetaInfo().setColumn(fields);
            });
        }
        return null;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        // 获取过滤SQL
        String queryFilterSql = getQueryFilterSql(commandConfig);
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return map;
        }

        // 获取查询SQL
        String querySql = table.getSql();

        // 存在条件
        if (StringUtil.isNotBlank(queryFilterSql)) {
            querySql += queryFilterSql;
        }
        PageSql pageSql = new PageSql(querySql, StringUtil.EMPTY, primaryKeys, table.getColumn());
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(pageSql));

        // 获取查询总数SQL
        map.put(SqlBuilderEnum.QUERY_COUNT.getName(), "SELECT COUNT(1) FROM (" + querySql + ") DBS_T");
        return map;
    }

    private boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        if (CollectionUtils.isEmpty(pk)) {
            return false;
        }
        return pk.stream().anyMatch(key -> key.equalsIgnoreCase(name));
    }

    public MetaInfo getTableMetaInfo(DatabaseConnectorInstance connectorInstance, String tableNamePattern) {
        return super.getMetaInfo(connectorInstance, tableNamePattern);
    }
}