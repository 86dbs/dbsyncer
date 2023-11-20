package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderUpdate extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        List<Field> fields = config.getFields();

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(config.getSchema());
        sql.append(quotation);
        sql.append(database.buildTableName(config.getTableName()));
        sql.append(quotation).append(" SET ");
        int size = fields.size();
        for (int i = 0; i < size; i++) {
            // skip pk
            if (fields.get(i).isPk()) {
                continue;
            }

            // "USERNAME"=?
            sql.append(quotation);
            sql.append(database.buildFieldName(fields.get(i)));
            sql.append(quotation).append("=?");
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
        List<String> primaryKeys = database.buildPrimaryKeys(config.getPrimaryKeys());
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " = ? ", true);
        return sql.toString();
    }

}