package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;
import java.util.Set;

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
        Set<String> primaryKeys = config.getPrimaryKeys();
        int size = fields.size();
        sql.append("UPDATE ").append(config.getSchema()).append(quotation).append(tableName).append(quotation).append(" SET ");
        for (int i = 0; i < size; i++) {
            // skip pk
            if (fields.get(i).isPk()) {
                continue;
            }

            // "USERNAME"=?
            sql.append(quotation).append(fields.get(i).getName()).append(quotation).append("=?");
            if (i < size - 1) {
                sql.append(",");
            }
        }

        // 删除多余的符号
        int last = sql.length() - 1;
        if(StringUtil.equals(",", sql.substring(last))){
            sql.deleteCharAt(last);
        }

        // UPDATE "USER" SET "USERNAME"=?,"AGE"=? WHERE "ID"=? AND "UID" = ?
        sql.append(" WHERE ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " = ? ", true);
        return sql.toString();
    }

}