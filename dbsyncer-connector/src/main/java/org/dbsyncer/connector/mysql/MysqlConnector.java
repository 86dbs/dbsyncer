package org.dbsyncer.connector.mysql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;

public final class MysqlConnector extends AbstractDatabaseConnector {

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String getPageSql(PageSql config) {
        final String quotation = config.getQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        final String quotation = config.getQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean hasFilter = StringUtil.isNotBlank(config.getSqlBuilderConfig().getQueryFilter());
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", !hasFilter);
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        Object[] cursors = config.getCursors();
        if (null == cursors) {
            return new Object[]{pageSize};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 1];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = pageSize;
        return newCursors;
    }

    @Override
    protected boolean enableCursor() {
        return true;
    }

}