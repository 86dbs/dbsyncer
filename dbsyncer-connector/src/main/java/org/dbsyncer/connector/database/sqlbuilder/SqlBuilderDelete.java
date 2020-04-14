package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderDelete implements SqlBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String buildSql(String tableName, String pk, List<String> filedNames, String queryFilter, Database database) {
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        // DELETE FROM USER WHERE USER.ID=?
        return new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(pk)
                .append("=?").toString();
    }

}