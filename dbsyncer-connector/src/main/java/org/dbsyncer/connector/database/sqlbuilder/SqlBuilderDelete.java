package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderDelete extends AbstractSqlBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String buildSql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        String quotation = config.getQuotation();
        // DELETE FROM "USER" WHERE "ID"=?
        return new StringBuilder().append("DELETE FROM ").append(quotation).append(tableName).append(quotation).append(" WHERE ").append(quotation).append(config.getPk()).append(quotation)
                .append("=?").toString();
    }

}