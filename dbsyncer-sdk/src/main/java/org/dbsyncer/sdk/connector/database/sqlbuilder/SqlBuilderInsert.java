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
 * @date 2019/9/26 23:58
 */
public class SqlBuilderInsert extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();

        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        for (Field f : config.getFields()) {
            // 添加字段名
            fs.add(database.buildWithQuotation(f.getName()));
            
            // 处理特殊类型的值表达式
            if (database.buildCustomValue(vs, f)) {
                continue;
            }
            vs.add("?");
        }
        // INSERT INTO "USER"("USERNAME","AGE") VALUES (?,?)
        return String.format("%sINSERT INTO %s%s(%s) VALUES (%s)",
                database.generateUniqueCode(),
                config.getSchema(),
                database.buildWithQuotation(config.getTableName()),
                StringUtil.join(fs, StringUtil.COMMA),
                StringUtil.join(vs, StringUtil.COMMA));
    }

}