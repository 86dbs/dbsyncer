package org.dbsyncer.connector.postgresql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Types;
import java.util.List;

@Component
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String TYPE = "PostgreSQL";

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @PostConstruct
    private void init() {
        VALUE_MAPPERS.put(Types.BIT, new PostgreSQLBitValueMapper());
        VALUE_MAPPERS.put(Types.OTHER, new PostgreSQLOtherValueMapper());
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        // select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid" limit ? OFFSET ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            logger.debug("不支持游标查询，主键包含非数字类型");
            return StringUtil.EMPTY;
        }

        // select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid" limit ? OFFSET ?
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(config.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        final List<String> primaryKeys = config.getPrimaryKeys();
        final String quotation = buildSqlWithQuotation();
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, " AND ", " > ? ", skipFirst);
        appendOrderByPk(config, sql);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageIndex = config.getPageIndex();
        int pageSize = config.getPageSize();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
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

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, primaryKeys, schema, queryFilterSql);
        }

        // 从系统表查询
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT N_LIVE_TUP FROM PG_STAT_USER_TABLES WHERE SCHEMANAME='%s' AND RELNAME='%s'", cfg.getSchema(), table.getName());
    }

    @Override
    public boolean enableCursor() {
        return true;
    }

}