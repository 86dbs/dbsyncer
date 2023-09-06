package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class SqlServerConnector extends AbstractDatabaseConnector {

    private static final String QUERY_VIEW = "select name from sysobjects where xtype in('v')";

    private static final String QUERY_TABLE = "select name from sys.tables where schema_id = schema_id('%s') and is_ms_shipped = 0";

    /**
     * 系统函数表达式convert/varchar/getdate
     */
    private final String SYS_EXPRESSION = "(convert\\().+?(\\))|(varchar\\().+?(\\))|(getdate\\(\\))";

    /**
     * 系统关键字段名
     */
    private final Set<String> SYS_FIELDS = CollectionUtils.newHashSet("convert", "user", "type", "version", "close", "bulk", "source", "current_date");

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        DatabaseConfig config = connectorMapper.getConfig();
        List<Table> tables = getTables(connectorMapper, String.format(QUERY_TABLE, config.getSchema()), TableTypeEnum.TABLE);
        tables.addAll(getTables(connectorMapper, QUERY_VIEW, TableTypeEnum.VIEW));
        return tables;
    }

    @Override
    public String getPageSql(PageSql config) {
        List<String> primaryKeys = config.getPrimaryKeys();
        String orderBy = StringUtil.join(primaryKeys, ",");
        return String.format(DatabaseConstant.SQLSERVER_PAGE_SQL, orderBy, config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[] {(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }

    @Override
    public String buildSqlFilterWithQuotation(String value) {
        // 支持SqlServer系统函数, Example: (select CONVERT(varchar(10),GETDATE(),120))
        if (containsKeyword(SYS_EXPRESSION, value)) {
            return StringUtil.EMPTY;
        }
        return super.buildSqlFilterWithQuotation(value);
    }

    @Override
    public String buildTableName(String tableName) {
        return containsKeyword(tableName) ? convertKey(tableName) : tableName;
    }

    @Override
    public String buildFieldName(Field field) {
        return containsKeyword(field.getName()) ? convertKey(field.getName()) : field.getName();
    }

    @Override
    public List<String> buildPrimaryKeys(List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return primaryKeys;
        }
        return primaryKeys.stream().map(pk -> containsKeyword(pk) ? convertKey(pk) : pk).collect(Collectors.toList());
    }

    @Override
    protected String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        // 视图或有过滤条件，走默认方式
        final Table table = commandConfig.getTable();
        if (StringUtil.isNotBlank(queryFilterSql) || TableTypeEnum.isView(table.getType())) {
            return super.getQueryCountSql(commandConfig, primaryKeys, schema, queryFilterSql);
        }

        DatabaseConfig cfg = (DatabaseConfig) commandConfig.getConnectorConfig();
        // 从存储过程查询（定时更新总数，可能存在误差）
        return String.format("select rows from sysindexes where id = object_id('%s.%s') and indid in (0, 1)", cfg.getSchema(),
                table.getName());
    }

    private List<Table> getTables(DatabaseConnectorMapper connectorMapper, String sql, TableTypeEnum type) {
        List<String> tableNames = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(sql, String.class));
        if (!CollectionUtils.isEmpty(tableNames)) {
            return tableNames.stream().map(name -> new Table(name, type.getCode())).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private String convertKey(String key) {
        return new StringBuilder("[").append(key).append("]").toString();
    }

    /**
     * 是否包含系统关键字
     *
     * @param val
     * @return
     */
    private boolean containsKeyword(String val) {
        if (StringUtil.isNotBlank(val)) {
            return SYS_FIELDS.contains(val.toLowerCase());
        }
        return false;
    }

    /**
     * 是否包含系统关键字
     *
     * @param regex
     * @param val
     * @return
     */
    private boolean containsKeyword(String regex, String val) {
        if (StringUtil.isNotBlank(val)) {
            Matcher matcher = Pattern.compile(regex).matcher(val.toLowerCase());
            return matcher.find();
        }
        return false;
    }
}