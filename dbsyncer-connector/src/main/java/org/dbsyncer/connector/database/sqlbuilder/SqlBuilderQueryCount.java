package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/8/8 0:03
 */
public class SqlBuilderQueryCount extends SqlBuilderQuery {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        String tableName = config.getTableName();
        List<String> primaryKeys = database.buildPrimaryKeys(config.getPrimaryKeys());
        String schema = config.getSchema();
        String queryFilter = config.getQueryFilter();

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(1) FROM (SELECT 1 AS ").append(quotation).append("_ROW").append(quotation).append(" FROM ");
        sql.append(schema);
        sql.append(quotation);
        sql.append(database.buildTableName(tableName));
        sql.append(quotation);
        if (StringUtil.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        sql.append(" GROUP BY ");
        // id,uid
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(") DBSYNCER_T");
        return sql.toString();
    }

}