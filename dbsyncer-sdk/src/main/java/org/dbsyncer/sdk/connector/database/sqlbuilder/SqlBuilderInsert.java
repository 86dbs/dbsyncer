/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/26 23:58
 */
public class SqlBuilderInsert extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String quotation = database.buildSqlWithQuotation();
        List<Field> fields = config.getFields();

        StringBuilder fs = new StringBuilder();
        StringBuilder vs = new StringBuilder();
        int size = fields.size();
        int end = size - 1;
        for (int i = 0; i < size; i++) {
            // "USERNAME"
            fs.append(quotation);
            fs.append(database.buildFieldName(fields.get(i)));
            fs.append(quotation);
            vs.append("?");
            //如果不是最后一个字段
            if (i < end) {
                fs.append(", ");
                vs.append(", ");
            }
        }
        // INSERT INTO "USER"("USERNAME","AGE") VALUES (?,?)
        StringBuilder sql = new StringBuilder(database.generateUniqueCode());
        sql.append("INSERT INTO ");
        sql.append(config.getSchema());
        sql.append(quotation);
        sql.append(database.buildTableName(config.getTableName()));
        sql.append(quotation);
        sql.append("(").append(fs).append(") VALUES (").append(vs).append(")");
        return sql.toString();
    }

}