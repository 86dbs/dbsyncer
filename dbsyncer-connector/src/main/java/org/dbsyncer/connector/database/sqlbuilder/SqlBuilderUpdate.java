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
public class SqlBuilderUpdate implements SqlBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String buildSql(String tableName, String pk, List<String> filedNames, String queryFilter, String quotation, Database database) {
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
        }
        StringBuilder sql = new StringBuilder();
        int size = filedNames.size();
        int end = size - 1;
        sql.append("UPDATE ").append(quotation).append(tableName).append(quotation).append(" SET ");
        for (int i = 0; i < size; i++) {
            // "USERNAME"=?
            sql.append(quotation).append(filedNames.get(i)).append(quotation).append("=?");
            //如果不是最后一个字段
            if (i < end) {
                sql.append(",");
            }
        }
        // UPDATE "USER" SET "USERNAME"=?,"AGE"=? WHERE "ID"=?
        sql.append(" WHERE ").append(quotation).append(pk).append(quotation).append("=?");
        return sql.toString();
    }

}