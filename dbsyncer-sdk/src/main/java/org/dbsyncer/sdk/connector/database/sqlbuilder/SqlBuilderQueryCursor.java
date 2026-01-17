/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;

import java.util.List;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026/01/17 0:03
 */
public class SqlBuilderQueryCursor extends SqlBuilderQuery {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        String queryFilter = config.getQueryFilter();
        List<String> primaryKeys = config.getPrimaryKeys();
        List<Field> fields = config.getFields();
        String querySql = buildQuerySql(config);
        PageSql pageSql = new PageSql(querySql, queryFilter, primaryKeys, fields);
        return config.getDatabase().getPageCursorSql(pageSql);
    }

}