/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.SqlTable;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

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
            if (StringUtil.equals(s.getSqlName(), sqlName)) {
                String sql = s.getSql().toUpperCase();
                sql = sql.replace("\t", " ");
                sql = sql.replace("\r", " ");
                sql = sql.replace("\n", " ");
                String queryMetaSql = StringUtil.contains(sql, " WHERE ") ? s.getSql() + " AND 1!=1 " : s.getSql() + " WHERE 1!=1 ";
                return connectorInstance.execute(databaseTemplate -> super.getMetaInfo(databaseTemplate, queryMetaSql, getSchema(cfg), s.getTable()));
            }
        }
        return null;
    }

    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
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
        
        // 流式查询SQL（DQL直接使用用户自定义的SQL）
        map.put(ConnectorConstant.OPERTION_QUERY_STREAM, querySql);

        // 获取查询总数SQL
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, "SELECT COUNT(1) FROM (" + querySql + ") DBS_T");
        return map;
    }

    public MetaInfo getTableMetaInfo(DatabaseConnectorInstance connectorInstance, String tableNamePattern) {
        return super.getMetaInfo(connectorInstance, tableNamePattern);
    }
}