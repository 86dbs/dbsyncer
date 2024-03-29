/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.Database;

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
        String schema = config.getSchema();
        String queryFilter = config.getQueryFilter();

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(1) FROM ");
        sql.append(schema);
        sql.append(quotation);
        sql.append(database.buildTableName(tableName));
        sql.append(quotation);
        if (StringUtil.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        return sql.toString();
    }

}