/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
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

        StringBuilder sql = new StringBuilder(database.generateUniqueCode());
        sql.append("UPDATE ").append(config.getSchema());
        sql.append(database.buildWithQuotation(config.getTableName()));
        sql.append(" SET ");

        List<String> fs = new ArrayList<>();
        for (Field f : config.getFields()) {
            String fieldName = database.buildWithQuotation(f.getName());

            // 处理特殊类型的值表达式
            List<String> vs = new ArrayList<>();
            if (database.buildCustomValue(vs, f)) {
                // 使用自定义值表达式
                fs.add(fieldName + "=" + vs.get(0));
            } else {
                // 使用默认的 ? 占位符
                fs.add(fieldName + "=?");
            }
        }
        sql.append(StringUtil.join(fs, StringUtil.COMMA));

        // UPDATE "USER" SET "USERNAME"=?,"AGE"=? WHERE "ID"=? AND "UID" = ?
        sql.append(" WHERE ");
        database.appendPrimaryKeys(sql, config.getPrimaryKeys());
        return sql.toString();
    }
}
