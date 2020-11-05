package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
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
        String pk = config.getPk();

        if (StringUtils.isBlank(pk)) {
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        String tableName = config.getTableName();
        String quotation = config.getQuotation();
        // DELETE FROM "USER" WHERE "ID"=?
        return new StringBuilder().append("DELETE FROM ").append(quotation).append(tableName).append(quotation).append(" WHERE ").append(quotation).append(pk).append(quotation)
                .append("=?").toString();
    }

}