/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.doris.constant.DorisConstant;
import org.dbsyncer.connector.doris.load.DorisStreamLoadWriter;
import org.dbsyncer.connector.doris.schema.DorisSchemaResolver;
import org.dbsyncer.connector.doris.validator.DorisConfigValidator;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Apache Doris 连接器（兼容 MySQL 协议）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 21:00
 */
public final class DorisConnector extends MySQLConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Set<String> SYSTEM_DATABASES = Stream.of(
            "information_schema", "mysql", "__internal_schema")
            .collect(Collectors.toSet());

    /**
     * 匹配列定义末尾的 MySQL 风格 PRIMARY KEY 子句，Doris 建表前需剥离。
     */
    private static final Pattern PRIMARY_KEY_CLAUSE = Pattern.compile(
            ",?\\s*PRIMARY\\s+KEY\\s*\\(([^)]+)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private final DorisConfigValidator configValidator = new DorisConfigValidator();
    private final DorisSchemaResolver schemaResolver = new DorisSchemaResolver();
    private final DorisStreamLoadWriter streamLoadWriter = new DorisStreamLoadWriter();

    @Override
    public String getConnectorType() {
        return "Doris";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        DorisConstant.enrichJdbcProperties(config);
        return super.connect(config, context);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        DatabaseConfig config = connectorInstance.getConfig();
        if (DorisConstant.isStreamLoadMode(config) && DorisConstant.isStreamLoadEvent(context.getEvent())) {
            return streamLoadWriter.write(connectorInstance, context);
        }
        return super.writer(connectorInstance, context);
    }

    /**
     * Doris 不支持 MySQL 的 ON DUPLICATE KEY UPDATE；Unique/Primary Key 模型下普通 INSERT 即按键覆盖。
     */
    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        return buildPlainInsertSql(config);
    }

    /**
     * Doris 不支持 INSERT IGNORE，使用普通 INSERT。
     */
    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        return buildPlainInsertSql(config);
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public StorageService getStorageService() {
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate -> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (CollectionUtils.isEmpty(databases)) {
                return Collections.emptyList();
            }
            return databases.stream()
                    .filter(name -> !SYSTEM_DATABASES.contains(name.toLowerCase()))
                    .collect(Collectors.toList());
        });
    }

    /**
     * 列类型仍走 JDBC {@code getColumns}；键信息用 {@code SHOW FULL COLUMNS}
     *（避免 information_schema 预编译参数在 Doris 上触发 MEM_ALLOC_FAILED）。
     */
    @Override
    public List<MetaInfo> getMetaInfo(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        if (CollectionUtils.isEmpty(context.getTablePatterns())) {
            return Collections.emptyList();
        }
        for (Table table : context.getTablePatterns()) {
            if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                return super.getMetaInfo(connectorInstance, context);
            }
        }
        List<MetaInfo> metaInfos = super.getMetaInfo(connectorInstance, context);
        if (CollectionUtils.isEmpty(metaInfos)) {
            return metaInfos;
        }
        String database = StringUtil.getIfBlank(context.getCatalog(), context.getSchema());
        if (StringUtil.isBlank(database)) {
            return metaInfos;
        }
        return connectorInstance.execute(databaseTemplate -> {
            for (MetaInfo metaInfo : metaInfos) {
                if (metaInfo == null || StringUtil.isBlank(metaInfo.getTable())
                        || CollectionUtils.isEmpty(metaInfo.getColumn())) {
                    continue;
                }
                try {
                    String sql = "SHOW FULL COLUMNS FROM " + buildWithQuotation(database) + "."
                            + buildWithQuotation(metaInfo.getTable());
                    applyDorisKeyFlags(databaseTemplate.queryForList(sql), metaInfo);
                } catch (Exception e) {
                    // 键标记失败不阻断元数据加载，下游无主键时再由业务报错
                    logger.warn("获取 Doris 键列失败, database={}, table={}, err={}",
                            database, metaInfo.getTable(), e.getMessage());
                }
            }
            return metaInfos;
        });
    }

    @Override
    public String buildCreateDatabaseSql(String databaseName, String schemaName) {
        if (StringUtil.isBlank(databaseName)) {
            return StringUtil.EMPTY;
        }
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

    /**
     * Doris 建表必须指定 KEY 模型与分桶。跨库迁移默认使用明细模型（DUPLICATE KEY）：
     * 优先取源 PRIMARY KEY 列（并重排为 schema 有序前缀），否则回退首列。
     */
    @Override
    public String getTargetTableDDL(DatabaseConnectorInstance targetInstance, String tableName, String sourceDDL) {
        if (StringUtil.isBlank(sourceDDL)) {
            return StringUtil.EMPTY;
        }
        String trimmed = sourceDDL.trim();
        List<String> keyColumns = extractPrimaryKeyColumns(trimmed);
        String columns = stripPrimaryKeyClause(trimmed);
        if (CollectionUtils.isEmpty(keyColumns)) {
            String firstColumn = extractFirstColumnName(columns);
            if (StringUtil.isBlank(firstColumn)) {
                return StringUtil.EMPTY;
            }
            keyColumns = Collections.singletonList(firstColumn);
        } else {
            columns = reorderColumnsForKeyPrefix(columns, keyColumns);
        }
        String qualifiedTable = qualifyTableName(targetInstance, tableName);
        String keyList = keyColumns.stream().map(this::buildWithQuotation).collect(Collectors.joining(StringUtil.COMMA));
        String hashCol = buildWithQuotation(keyColumns.get(0));
        return String.format(Locale.ROOT,
                "CREATE TABLE IF NOT EXISTS %s (%s) DUPLICATE KEY(%s) DISTRIBUTED BY HASH(%s) BUCKETS 10 "
                        + "PROPERTIES (\"replication_num\" = \"1\")",
                qualifiedTable, columns, keyList, hashCol);
    }

    private static void applyDorisKeyFlags(List<Map<String, Object>> keyRows, MetaInfo metaInfo) {
        if (CollectionUtils.isEmpty(keyRows)) {
            return;
        }
        List<String> keyColumns = new ArrayList<>();
        for (Map<String, Object> row : keyRows) {
            // SHOW FULL COLUMNS: Field / Key；兼容 information_schema 列名
            String columnName = getMapString(row, "Field", "FIELD", "field", "COLUMN_NAME", "column_name");
            if (StringUtil.isBlank(columnName)) {
                continue;
            }
            String columnKey = getMapString(row, "Key", "KEY", "key", "COLUMN_KEY", "column_key");
            if (isDorisKeyColumn(columnKey)) {
                keyColumns.add(columnName);
            }
        }
        if (CollectionUtils.isEmpty(keyColumns)) {
            return;
        }
        Set<String> keySet = keyColumns.stream()
                .map(name -> name.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(HashSet::new));
        for (Field field : metaInfo.getColumn()) {
            if (field == null || StringUtil.isBlank(field.getName())) {
                continue;
            }
            field.setPk(keySet.contains(field.getName().toUpperCase(Locale.ROOT)));
        }
    }

    /**
     * Doris {@code SHOW FULL COLUMNS} 的 Key 常为 YES/NO；部分版本/information_schema 为 PRI/UNI/AGG/DUP。
     */
    private static boolean isDorisKeyColumn(String columnKey) {
        if (StringUtil.isBlank(columnKey)) {
            return false;
        }
        String key = columnKey.trim().toUpperCase(Locale.ROOT);
        return "YES".equals(key) || "TRUE".equals(key) || "1".equals(key)
                || "PRI".equals(key) || "UNI".equals(key) || "AGG".equals(key) || "DUP".equals(key);
    }

    private static String getMapString(Map<String, Object> row, String... keys) {
        if (row == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            Object value = row.get(key);
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private static String stripPrimaryKeyClause(String sourceDDL) {
        Matcher pkMatcher = PRIMARY_KEY_CLAUSE.matcher(sourceDDL);
        if (!pkMatcher.find()) {
            return sourceDDL;
        }
        String columns = pkMatcher.replaceFirst(StringUtil.EMPTY).trim();
        if (columns.endsWith(StringUtil.COMMA)) {
            columns = columns.substring(0, columns.length() - 1).trim();
        }
        return columns;
    }

    private static List<String> extractPrimaryKeyColumns(String sourceDDL) {
        Matcher pkMatcher = PRIMARY_KEY_CLAUSE.matcher(sourceDDL);
        if (!pkMatcher.find()) {
            return Collections.emptyList();
        }
        String inside = pkMatcher.group(1);
        if (StringUtil.isBlank(inside)) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        for (String part : inside.split(StringUtil.COMMA)) {
            String name = unwrapIdentifier(part.trim());
            if (StringUtil.isNotBlank(name)) {
                keys.add(name);
            }
        }
        return keys;
    }

    /**
     * Doris 要求 KEY 列为 schema 有序前缀；将主键列定义移到列清单最前。
     */
    private static String reorderColumnsForKeyPrefix(String columns, List<String> keyColumns) {
        List<String> defs = splitColumnDefinitions(columns);
        if (CollectionUtils.isEmpty(defs) || CollectionUtils.isEmpty(keyColumns)) {
            return columns;
        }
        Set<String> keySet = keyColumns.stream()
                .map(name -> name.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<String> keyDefs = new ArrayList<>();
        List<String> otherDefs = new ArrayList<>();
        Set<String> foundKeys = new HashSet<>();
        for (String def : defs) {
            String colName = extractColumnNameFromDef(def);
            if (StringUtil.isNotBlank(colName) && keySet.contains(colName.toUpperCase(Locale.ROOT))) {
                keyDefs.add(def);
                foundKeys.add(colName.toUpperCase(Locale.ROOT));
            } else {
                otherDefs.add(def);
            }
        }
        if (foundKeys.size() != keySet.size()) {
            return columns;
        }
        List<String> ordered = new ArrayList<>(keyDefs.size() + otherDefs.size());
        // 按 PRIMARY KEY 声明顺序排放键列
        for (String key : keyColumns) {
            String upper = key.toUpperCase(Locale.ROOT);
            for (String def : keyDefs) {
                String colName = extractColumnNameFromDef(def);
                if (StringUtil.isNotBlank(colName) && upper.equals(colName.toUpperCase(Locale.ROOT))) {
                    ordered.add(def);
                    break;
                }
            }
        }
        ordered.addAll(otherDefs);
        return StringUtil.join(ordered, StringUtil.COMMA);
    }

    private static List<String> splitColumnDefinitions(String columns) {
        List<String> result = new ArrayList<>();
        if (StringUtil.isBlank(columns)) {
            return result;
        }
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < columns.length(); i++) {
            char c = columns.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            } else if (c == ',' && depth == 0) {
                String part = current.toString().trim();
                if (StringUtil.isNotBlank(part)) {
                    result.add(part);
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        String last = current.toString().trim();
        if (StringUtil.isNotBlank(last)) {
            result.add(last);
        }
        return result;
    }

    private static String extractColumnNameFromDef(String columnDef) {
        if (StringUtil.isBlank(columnDef)) {
            return StringUtil.EMPTY;
        }
        String[] tokens = columnDef.trim().split("\\s+");
        return unwrapIdentifier(tokens[0]);
    }

    @Override
    public String getSourceTableDDL(DatabaseConnectorInstance sourceInstance, String sourceTableName) {
        // Doris 兼容 MySQL 协议，SHOW CREATE TABLE 返回格式与 MySQL 一致
        return super.getSourceTableDDL(sourceInstance, sourceTableName);
    }

    @Override
    public String buildDropTableSql(DatabaseConnectorInstance targetInstance, String tableName) {
        if (StringUtil.isBlank(tableName)) {
            return StringUtil.EMPTY;
        }
        return "DROP TABLE IF EXISTS " + qualifyTableName(targetInstance, tableName);
    }

    private String qualifyTableName(DatabaseConnectorInstance targetInstance, String tableName) {
        String qualifiedTable = buildWithQuotation(tableName);
        if (targetInstance == null) {
            return qualifiedTable;
        }
        String database = StringUtil.isNotBlank(targetInstance.getCatalog())
                ? targetInstance.getCatalog()
                : targetInstance.getSchema();
        if (StringUtil.isNotBlank(database)) {
            return buildWithQuotation(database) + "." + qualifiedTable;
        }
        return qualifiedTable;
    }

    private static String extractFirstColumnName(String columns) {
        List<String> defs = splitColumnDefinitions(columns);
        if (CollectionUtils.isEmpty(defs)) {
            return StringUtil.EMPTY;
        }
        return extractColumnNameFromDef(defs.get(0));
    }

    private static String unwrapIdentifier(String identifier) {
        if (StringUtil.isBlank(identifier)) {
            return StringUtil.EMPTY;
        }
        String name = identifier.trim();
        if ((name.startsWith("`") && name.endsWith("`"))
                || (name.startsWith("\"") && name.endsWith("\""))
                || (name.startsWith("[") && name.endsWith("]"))) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    private String buildPlainInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<String> placeholders = new ArrayList<>(fields.size());
        for (Field field : fields) {
            fieldNames.add(database.buildWithQuotation(field.getName()));
            placeholders.add("?");
        }
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        return String.format("%sINSERT INTO %s (%s) VALUES (%s)",
                database.generateUniqueCode(),
                table,
                StringUtil.join(fieldNames, StringUtil.COMMA),
                StringUtil.join(placeholders, StringUtil.COMMA));
    }
}
