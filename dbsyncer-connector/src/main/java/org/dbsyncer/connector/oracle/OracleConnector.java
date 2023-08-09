package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class OracleConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 系统函数表达式to_char/to_date/to_timestamp/to_number
     */
    private final String SYS_EXPRESSION = "(to_char\\().+?(\\))|(to_date\\().+?(\\))|(to_timestamp\\().+?(\\))|(to_number\\().+?(\\))";

    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        // SELECT * FROM (SELECT A.*, ROWNUM RN FROM (select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid")A WHERE ROWNUM <= ?) WHERE RN > ?
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        if (PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            appendOrderByPk(config, sql);
        }
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            logger.debug("不支持游标查询，主键包含非数字类型");
            return StringUtil.EMPTY;
        }

        // SELECT * FROM (SELECT A.*, ROWNUM RN FROM (select * from test."my_user" where "id" > ? and "uid" > ? order by "id","uid")A WHERE ROWNUM <= ?) WHERE RN > ?
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
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
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
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
    public boolean enableCursor() {
        return true;
    }

    @Override
    public String buildSqlFilterWithQuotation(String value) {
        if (StringUtil.isNotBlank(value)) {
            String val = value.toLowerCase();
            // 支持Oracle系统函数, Example: to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
            Matcher matcher = Pattern.compile(SYS_EXPRESSION).matcher(val);
            if (matcher.find()) {
                return StringUtil.EMPTY;
            }
        }
        return super.buildSqlFilterWithQuotation(value);
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, primaryKeys, schema, queryFilterSql);
        }

        // 从系统表查询
        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        return String.format("SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'", cfg.getUsername().toUpperCase(), table.getName());
    }

}