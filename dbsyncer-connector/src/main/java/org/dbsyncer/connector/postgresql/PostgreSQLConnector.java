package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.List;
import java.util.Map;

public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public PostgreSQLConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        final String quotation = config.getQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        // select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid" limit ? OFFSET ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        final String quotation = config.getQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        final SqlBuilderConfig sqlBuilderConfig = config.getSqlBuilderConfig();
        Map<String, Integer> typeAliases = PrimaryKeyUtil.findPrimaryKeyType(sqlBuilderConfig.getFields());

        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(typeAliases, primaryKeys)) {
            logger.warn("不支持游标查询，主键包含非数字类型");
            return "";
        }

        // select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid" limit ? OFFSET ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(sqlBuilderConfig.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        // 通过游标分页
        if (config.isSupportedCursor()) {
            Object[] cursors = config.getCursors();
            if (null == cursors) {
                return new Object[]{pageSize, 0};
            }
            int cursorsLen = cursors.length;
            Object[] newCursors = new Object[cursorsLen + 2];
            System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
            newCursors[cursorsLen] = pageSize;
            newCursors[cursorsLen + 1] = 0;
            return newCursors;
        }

        // 普通分页
        return new Object[]{pageSize, (config.getPageIndex() - 1) * pageSize};
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