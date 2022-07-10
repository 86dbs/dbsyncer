package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.dbsyncer.connector.model.Field;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderUpdate extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        List<Field> fields = config.getFields();
        String quotation = config.getQuotation();
        StringBuilder sql = new StringBuilder();
        int size = fields.size();
        int end = size - 1;
        sql.append("UPDATE ").append(config.getSchema()).append(quotation).append(tableName).append(quotation).append(" SET ");
        for (int i = 0; i < size; i++) {
            // skip pk
            if(fields.get(i).isPk()){
               continue;
            }

            // "USERNAME"=?
            sql.append(quotation).append(fields.get(i).getName()).append(quotation).append("=?");
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