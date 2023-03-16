package org.dbsyncer.connector.sql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.SqlTable;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

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
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        DatabaseConfig cfg = connectorMapper.getConfig();
        List<SqlTable> sqlTables = cfg.getSqlTables();
        List<Table> tables = new ArrayList<>();
        if (!CollectionUtils.isEmpty(sqlTables)) {
            sqlTables.forEach(s ->
                tables.add(new Table(s.getSqlName(), TableTypeEnum.TABLE.getCode(), Collections.EMPTY_LIST, s.getSql()))
            );
        }
        return tables;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return getDqlSourceCommand(commandConfig, false);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorMapper connectorMapper, String sqlName) {
        DatabaseConfig cfg = connectorMapper.getConfig();
        List<SqlTable> sqlTables = cfg.getSqlTables();
        for (SqlTable s : sqlTables) {
            if (StringUtil.equals(s.getSqlName(), sqlName)) {
                String sql = s.getSql().toUpperCase();
                sql = sql.replace("\t", " ");
                sql = sql.replace("\r", " ");
                sql = sql.replace("\n", " ");
                String queryMetaSql = StringUtil.contains(sql, " WHERE ") ? s.getSql() + " AND 1!=1 " : s.getSql() + " WHERE 1!=1 ";
                return connectorMapper.execute(databaseTemplate -> super.getMetaInfo(databaseTemplate, queryMetaSql, getSchema(cfg), s.getTable()));
            }
        }
        return null;
    }

    /**
     * 获取DQL源配置
     *
     * @param commandConfig
     * @param groupByPK
     * @return
     */
    protected Map<String, String> getDqlSourceCommand(CommandConfig commandConfig, boolean groupByPK) {
        // 获取过滤SQL
        String queryFilterSql = getQueryFilterSql(commandConfig.getFilter());
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(commandConfig.getTable());
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return map;
        }

        // 获取查询SQL
        String querySql = table.getSql();

        // 存在条件
        if (StringUtil.isNotBlank(queryFilterSql)) {
            querySql += queryFilterSql;
        }
        String quotation = buildSqlWithQuotation();
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(new PageSql(querySql, quotation, primaryKeys)));

        // 获取查询总数SQL
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (").append(querySql);

        // Mysql
        if (groupByPK) {
            queryCount.append(" GROUP BY ");
            // id,id2
            PrimaryKeyUtil.buildSql(queryCount, primaryKeys, quotation, ",", "", true);
        }
        queryCount.append(") DBSYNCER_T");
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }
}