package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderDelete extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        String quotation = config.getQuotation();
        // DELETE FROM "USER" WHERE "ID"=?
        return new StringBuilder().append("DELETE FROM ").append(config.getSchema()).append(quotation).append(tableName).append(quotation).append(" WHERE ").append(quotation).append(config.getPk()).append(quotation)
                .append("=?").toString();
    }

}