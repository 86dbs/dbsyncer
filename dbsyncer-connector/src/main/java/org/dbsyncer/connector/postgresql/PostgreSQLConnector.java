package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;

import java.sql.Types;
import java.util.Set;

public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    public PostgreSQLConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        final String quotation = buildSqlWithQuotation();
        final Set<String> primaryKeys = config.getPrimaryKeys();
        // select * from test.`my_user` where `id` > ? and `uid` > ? order by `id`,`uid` limit ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean blank = StringUtil.isBlank(config.getSqlBuilderConfig().getQueryFilter());
        sql.append(blank ? " WHERE " : " AND ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", blank);
        sql.append(" ORDER BY ");
        // id,uid
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(" LIMIT ?");
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        final String quotation = buildSqlWithQuotation();
        final Set<String> primaryKeys = config.getPrimaryKeys();
        // select * from test.`my_user` order by `id`,`uid` limit ?
        StringBuilder sql = new StringBuilder(config.getQuerySql()).append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(" LIMIT ?");
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
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, schema, quotation, queryFilterSql);
        }

        // 从系统表查询
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT N_LIVE_TUP FROM PG_STAT_USER_TABLES WHERE SCHEMANAME='%s' AND RELNAME='%s'", cfg.getSchema(), table.getName());
    }

    @Override
    protected boolean enableCursor() {
        return true;
    }

}