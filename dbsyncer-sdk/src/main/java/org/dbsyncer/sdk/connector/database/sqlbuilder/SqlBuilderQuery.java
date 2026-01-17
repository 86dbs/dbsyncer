/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.AbstractSqlBuilder;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderQuery extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        // 分页语句
        List<String> primaryKeys = config.getPrimaryKeys();
        String queryFilter = config.getQueryFilter();
        List<Field> fields = config.getFields();
        PageSql pageSql = new PageSql(buildQuerySql(config), queryFilter, primaryKeys, fields);
        return config.getDatabase().getPageSql(pageSql);
    }

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<String> fs = new ArrayList<>();
        for (Field f : config.getFields()) {
            // 使用了别名
            if (StringUtil.isNotBlank(f.getLabelName())) {
                fs.add(database.buildWithQuotation(f.getName()) + " AS " + f.getLabelName());
                continue;
            }
            fs.add(database.buildWithQuotation(f.getName()));
        }

        // SELECT "ID","NAME" FROM "TEST"."MY_USER" WHERE "ID" > 100
        return String.format("SELECT %s FROM %s%s%s",
                StringUtil.join(fs, StringUtil.COMMA),
                config.getSchema(),
                database.buildWithQuotation(config.getTableName()),
                config.getQueryFilter());
    }

}