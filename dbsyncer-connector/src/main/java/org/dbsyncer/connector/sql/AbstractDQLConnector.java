package org.dbsyncer.connector.sql;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.SqlTable;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.ArrayList;
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
            sqlTables.forEach(s -> {
                Table table = new Table(s.getTable());
                table.setSql(s.getSql());
                tables.add(table);
            });
        }
        return tables;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return getDqlSourceCommand(commandConfig, false);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorMapper connectorMapper, String tableName) {
        DatabaseConfig cfg = connectorMapper.getConfig();
        List<SqlTable> sqlTables = cfg.getSqlTables();
        for (SqlTable s : sqlTables) {
            if (s.getTable().equals(tableName)) {
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
        String primaryKey = PrimaryKeyUtil.findOriginalTablePrimaryKey(commandConfig.getOriginalTable());

        // 获取查询SQL
        Map<String, String> map = new HashMap<>();
        String querySql = table.getSql();

        // 存在条件
        if (StringUtil.isNotBlank(queryFilterSql)) {
            querySql += queryFilterSql;
        }
        String quotation = buildSqlWithQuotation();
        String pk = new StringBuilder(quotation).append(primaryKey).append(quotation).toString();
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(new PageSql(querySql, pk)));

        // 获取查询总数SQL
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (").append(querySql);

        // Mysql
        if (groupByPK) {
            queryCount.append(" GROUP BY ").append(pk);
        }
        queryCount.append(") DBSYNCER_T");
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }
}