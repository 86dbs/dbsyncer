package org.dbsyncer.connector.mysql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.util.List;
import java.util.Map;

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
        final SqlBuilderConfig sqlBuilderConfig = config.getSqlBuilderConfig();
        Map<String, Integer> typeAliases = PrimaryKeyUtil.findPrimaryKeyType(sqlBuilderConfig.getFields());

        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(sqlBuilderConfig.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        PrimaryKeyUtil.buildCursorSql(sql, primaryKeys, quotation, " AND ", typeAliases, skipFirst);
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
            return new Object[]{0, pageSize};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 2];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = 0;
        newCursors[cursorsLen + 1] = pageSize;
        return newCursors;
    }

    @Override
    protected boolean enableCursor() {
        return true;
    }

}