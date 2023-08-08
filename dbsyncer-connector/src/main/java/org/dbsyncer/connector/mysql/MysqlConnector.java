package org.dbsyncer.connector.mysql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class MysqlConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String getPageSql(PageSql config) {
        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            logger.debug("不支持游标查询，主键包含非数字类型");
            return StringUtil.EMPTY;
        }

        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?,?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(config.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        final String quotation = buildSqlWithQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
        appendOrderByPk(config, sql);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderConfig config) {
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
    public boolean enableCursor() {
        return true;
    }

}