/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbase.cdc.OceanBaseListener;
import org.dbsyncer.connector.oceanbase.schema.OceanBaseSchemaResolver;
import org.dbsyncer.connector.oceanbase.validator.OceanBaseConfigValidator;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OceanBase连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-04 00:20
 */
public final class OceanBaseConnector extends AbstractDatabaseConnector {

    private static final String QUERY_TABLES = "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES "
            + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')";

    private final OceanBaseConfigValidator configValidator = new OceanBaseConfigValidator();
    private final OceanBaseSchemaResolver schemaResolver = new OceanBaseSchemaResolver();
    private final Set<String> SYSTEM_DATABASES = Stream.of(
                    // MySQL 兼容系统库
                    "information_schema", "mysql", "performance_schema", "sys",
                    // OceanBase 系统库
                    "oceanbase", "__public", "__recyclebin", "ocs",
                    // Oracle 兼容系统库（企业版 / 混合租户常见）
                    "appqossys", "audsys", "ctxsys", "dbsfwuser", "dbsnmp", "dvsys",
                    "gsmadmin_internal", "lbacsys", "mdsys", "ojvmsys", "olapsys",
                    "orddata", "ordsys", "oraauditor", "outln", "system",
                    "sys_external_tbs", "wmsys", "xdb")
            .collect(Collectors.toSet());

    @Override
    public String getConnectorType() {
        return "OceanBase";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new OceanBaseListener();
        }
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (!CollectionUtils.isEmpty(databases)) {
                return databases.stream().filter(name -> !SYSTEM_DATABASES.contains(name.toLowerCase())).collect(Collectors.toList());
            }
            return Collections.emptyList();
        });
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        String database = context.getCatalog();
        if (StringUtil.isBlank(database)) {
            return Collections.emptyList();
        }
        return connectorInstance.execute(databaseTemplate -> {
            List<Map<String, Object>> rows = databaseTemplate.queryForList(QUERY_TABLES, database);
            if (CollectionUtils.isEmpty(rows)) {
                return Collections.emptyList();
            }
            List<Table> tables = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object nameValue = row.get("TABLE_NAME");
                if (nameValue == null) {
                    continue;
                }
                Table table = new Table();
                table.setName(String.valueOf(nameValue));
                table.setType(resolveTableType(row.get("TABLE_TYPE")));
                tables.add(table);
            }
            return tables;
        });
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        return "CREATE DATABASE IF NOT EXISTS " + buildWithQuotation(databaseName);
    }

    @Override
    public boolean databaseExists(DatabaseConnectorInstance connectorInstance, String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return false;
        }
        return connectorInstance.execute(databaseTemplate ->
                !CollectionUtils.isEmpty(databaseTemplate.queryForList("SHOW DATABASES LIKE ?", String.class, databaseName)));
    }

    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + sourceDDL + ")";
    }

    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        if (sourceInstance == null || StringUtil.isBlank(sourceTableName)) {
            return StringUtil.EMPTY;
        }
        String sql = "SHOW CREATE TABLE " + buildWithQuotation(sourceTableName);
        return sourceInstance.execute(databaseTemplate -> {
            List<java.util.Map<String, Object>> rows = databaseTemplate.queryForList(sql);
            if (CollectionUtils.isEmpty(rows)) {
                return StringUtil.EMPTY;
            }
            java.util.Map<String, Object> ddlRow = rows.get(0);
            if (ddlRow == null || ddlRow.isEmpty()) {
                return StringUtil.EMPTY;
            }
            String ddl = StringUtil.EMPTY;
            for (java.util.Map.Entry<String, Object> entry : ddlRow.entrySet()) {
                if (entry.getKey() != null && entry.getKey().equalsIgnoreCase("create table")
                        && entry.getValue() != null) {
                    ddl = String.valueOf(entry.getValue());
                    break;
                }
            }
            if (StringUtil.isBlank(ddl)) {
                return StringUtil.EMPTY;
            }
            return ddl.replaceFirst("(?i)^CREATE\\s+TABLE\\s+", "CREATE TABLE IF NOT EXISTS ");
        });
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        return "DROP TABLE IF EXISTS " + buildWithQuotation(tableName);
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类方法添加ORDER BY（按主键排序，保证分页一致性）
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{0, pageSize};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{0, pageSize};
        }
        // OceanBase需要OFFSET=0和LIMIT=pageSize参数
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = 0; // OFFSET
        newCursors[cursorArgs.length + 1] = pageSize; // LIMIT
        return newCursors;
    }

    @Override
    public String buildModifyColumnsSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                        String targetTableName, List<Field> sourceDefinitions,
                                        List<String> targetColumnNames) {
        if (CollectionUtils.isEmpty(sourceDefinitions) || CollectionUtils.isEmpty(targetColumnNames)) {
            return StringUtil.EMPTY;
        }
        int loopSize = Math.min(sourceDefinitions.size(), targetColumnNames.size());
        //拼接数据库和表名 db.table
        String qualifiedTable = qualifyTable(targetInstance, task, targetTableName);
        List<String> clauses = new ArrayList<>(loopSize);
        for (int i = 0; i < loopSize; i++) {
            Field sourceField = sourceDefinitions.get(i);
            String targetColumn = targetColumnNames.get(i);
            // 非法数据直接跳过
            if (sourceField == null || StringUtil.isBlank(targetColumn)) {
                continue;
            }
            String col = buildWithQuotation(targetColumn);
            String type = formatPhysicalType(sourceField);
            clauses.add(String.format(Locale.ROOT, "MODIFY COLUMN %s %s", col, type));
        }
        if (clauses.isEmpty()) {
            return StringUtil.EMPTY;
        }
        return String.format(Locale.ROOT, "ALTER TABLE %s %s", qualifiedTable, StringUtil.join(clauses, ", "));
    }

    private String qualifyTable(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                String tableName) {
        String dbName = StringUtil.isNotBlank(targetInstance.getCatalog())
                ? targetInstance.getCatalog()
                : task.getTargetDatabase();
        if (StringUtil.isBlank(dbName)) {
            return buildWithQuotation(tableName);
        }
        return buildWithQuotation(dbName) + "." + buildWithQuotation(tableName);
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        List<String> dfs = new ArrayList<>();
        fields.forEach(f -> {
            String name = database.buildWithQuotation(f.getName());
            fs.add(name);
            vs.add("?");
            if (!f.isPk()) {
                dfs.add(String.format("%s = VALUES(%s)", name, name));
            }
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);
        String dupNames = StringUtil.join(dfs, StringUtil.COMMA);
        // 基于主键或唯一索引冲突时更新
        return String.format("%sINSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;", uniqueCode, table, fieldNames, values, dupNames);
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();

        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        fields.forEach(f -> {
            fs.add(database.buildWithQuotation(f.getName()));
            vs.add("?");
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);

        // 冲突时忽略插入，不进行任何操作
        return String.format("%sINSERT IGNORE INTO %s (%s) VALUES (%s)", uniqueCode, table, fieldNames, values);
    }

    private StringBuilder buildTableName(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        return table;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return null;
    }

    private String resolveTableType(Object tableType) {
        if (tableType != null && StringUtil.equalsIgnoreCase(TableTypeEnum.VIEW.getCode(), String.valueOf(tableType))) {
            return TableTypeEnum.VIEW.getCode();
        }
        return TableTypeEnum.TABLE.getCode();
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:oceanbase://127.0.0.1:2881/test
        StringBuilder url = new StringBuilder();
        url.append("jdbc:oceanbase://").append(config.getHost()).append(":").append(config.getPort());
        if (database != null && !database.trim().isEmpty()) {
            url.append("/").append(database);
        }
        return url.toString();
    }

    @Override
    public String buildAlterCatalog(DatabaseConnectorInstance connectorInstance, Alter alter) {
        // 目标数据库名
        String catalog = connectorInstance.getCatalog();
        catalog = buildWithQuotation(catalog);
        // 1. 生成基础 SQL
        String sql = alter.toString();

        // 如果目标库名不为空且当前 SQL 未包含该库名
        if (catalog != null && !sql.contains(catalog + ".")) {
            String tableName = alter.getTable().getName();

            // 正则解释：
            // (?i) : 忽略大小写
            // (ALTER\s+TABLE\s+) : 捕获组1，匹配 "ALTER TABLE " 及其后的空格
            // (?:`[^`]+`\.)? : 非捕获组，匹配可选的 "旧库名." (例如 `test`.)
            // (?:`)? : 匹配可选的起始反引号
            // \\Q...\\E : 匹配纯表名
            // (?:`)? : 匹配可选的结束反引号
            String regex = "(?i)(ALTER\\s+TABLE\\s+)(?:`[^`]+`\\.)?(?:`)?" + java.util.regex.Pattern.quote(tableName) + "(?:`)?";

            // 替换为：捕获组1 + 新库名 + . + 表名
            String replacement = "$1" + catalog + "." + tableName;
            return sql.replaceFirst(regex, replacement);
        }
        return sql;
    }

    @Override
    public String formatPhysicalType(Field sourceDefinition) {
        if (sourceDefinition == null || StringUtil.isBlank(sourceDefinition.getTypeName())) {
            return super.formatPhysicalType(sourceDefinition);
        }
        String t = sourceDefinition.getTypeName().trim().toUpperCase(Locale.ROOT);
        // MODIFY COLUMN 下 ENUM/SET 若无枚举字面量列表则非法，改为 VARCHAR
        if ("ENUM".equals(t) || "SET".equals(t)) {
            int len = sourceDefinition.getColumnSize() > 0 ? sourceDefinition.getColumnSize() : 255;
            return String.format(Locale.ROOT, "VARCHAR(%d)", len);
        }
        return super.formatPhysicalType(sourceDefinition);
    }

}