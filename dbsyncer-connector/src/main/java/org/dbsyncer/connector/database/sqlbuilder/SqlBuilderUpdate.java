package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderUpdate extends AbstractSqlBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String buildSql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        List<String> filedNames = config.getFiledNames();
        String quotation = config.getQuotation();
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
        sql.append(" WHERE ").append(quotation).append(config.getPk()).append(quotation).append("=?");
        return sql.toString();
    }

}