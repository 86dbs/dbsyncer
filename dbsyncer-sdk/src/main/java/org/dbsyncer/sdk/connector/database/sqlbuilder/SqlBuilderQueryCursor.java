/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/9 0:03
 */
public class SqlBuilderQueryCursor extends SqlBuilderQuery {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        // 分页语句
        Database database = config.getDatabase();
        String queryFilter = config.getQueryFilter();
        List<String> primaryKeys = database.buildPrimaryKeys(config.getPrimaryKeys());
        List<Field> fields = config.getFields();
        PageSql pageSql = new PageSql(buildQuerySql(config), queryFilter, primaryKeys, fields);
        return config.getDatabase().getPageCursorSql(pageSql);
    }

}