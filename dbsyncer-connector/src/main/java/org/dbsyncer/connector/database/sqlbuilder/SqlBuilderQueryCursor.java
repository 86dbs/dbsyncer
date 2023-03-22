package org.dbsyncer.connector.database.sqlbuilder;

import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.model.PageSql;

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
        PageSql pageSql = new PageSql(config, buildQuerySql(config), config.getQuotation(), config.getPrimaryKeys());
        return database.getPageCursorSql(pageSql);
    }

}