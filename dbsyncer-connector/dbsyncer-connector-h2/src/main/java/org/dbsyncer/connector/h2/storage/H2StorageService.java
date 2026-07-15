/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2.storage;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UnderlineToCamelUtils;
import org.dbsyncer.connector.h2.H2Connector;
import org.dbsyncer.connector.h2.H2Exception;
import org.dbsyncer.sdk.NullExecutorException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.storage.AbstractStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 将存储数据写入 H2（嵌入式）
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020-01-08 15:17
 */
public class H2StorageService extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String PREFIX_TABLE = "dbsyncer_";
    private final String DROP_TABLE = "DROP TABLE %s";
    private final String TRUNCATE_TABLE = "TRUNCATE TABLE %s";
    private final String QUERY_TABLE_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER(?)";
    private final String QUERY_COLUMN_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = UPPER(?) AND UPPER(COLUMN_NAME) = UPPER(?)";
    private final String QUERY_INDEX_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.INDEXES WHERE UPPER(TABLE_NAME) = UPPER(?) AND UPPER(INDEX_NAME) = UPPER(?)";
    private final String QUERY_INDEX_COLUMN_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE UPPER(TABLE_NAME) = UPPER(?) AND UPPER(INDEX_NAME) = UPPER(?) AND UPPER(COLUMN_NAME) = UPPER(?)";
    private final String UK_DATABASE_SYNC_DETAIL = "UK_TASK_TYPE_TABLE";
    private final String UK_DATABASE_SYNC_DETAIL_COLUMNS = "`TASK_ID`,`TYPE`,`SOURCE_DATABASE`,`SOURCE_SCHEMA`,`TARGET_DATABASE`,`TARGET_SCHEMA`,`TABLE_INDEX`";

    private final H2Connector connector = new H2Connector();
    private final Map<String, Executor> tables = new ConcurrentHashMap<>();
    private DatabaseConnectorInstance connectorInstance;

    @Override
    public void init(Properties properties) {
        DatabaseConfig config = new DatabaseConfig();
        config.setConnectorType(properties.getProperty("dbsyncer.storage.type"));
        String url = properties.getProperty("dbsyncer.storage.h2.url");
        String username = properties.getProperty("dbsyncer.storage.h2.username", "sa");
        String password = properties.getProperty("dbsyncer.storage.h2.password", StringUtil.EMPTY);
        String driverClassName = properties.getProperty("dbsyncer.storage.h2.driver-class-name", "org.h2.Driver");
        config.setUsername(StringUtil.replace(username.trim(), "\t", StringUtil.EMPTY));
        config.setPassword(StringUtil.replace(password.trim(), "\t", StringUtil.EMPTY));
        config.setDriverClassName(driverClassName);
        config.setUrl(url);
        logger.info("h2 storage url:{}", url);
        connectorInstance = new DatabaseConnectorInstance(config);
        ensureSchema();
        initTable();
    }

    /**
     * MODE=MySQL 下 H2 默认在 PUBLIC schema 建表，启动时显式确保 schema 存在。
     */
    private void ensureSchema() {
        connectorInstance.execute(databaseTemplate -> {
            databaseTemplate.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC");
            databaseTemplate.execute("SET SCHEMA PUBLIC");
            return null;
        });
    }

    @Override
    protected String getSeparator() {
        return StringUtil.UNDERLINE;
    }

    @Override
    protected Paging select(String sharding, Query query) {
        Paging paging = new Paging(query.getPageNum(), query.getPageSize());
        Executor executor = getExecutor(query.getType(), sharding);
        if (executor == null) {
            return paging;
        }
        List<Object> queryCountArgs = new ArrayList<>();
        String queryCountSql = buildQueryCountSql(query, executor, queryCountArgs);
        Long total = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject(queryCountSql, queryCountArgs.toArray(), Long.class));
        paging.setTotal(total);
        if (query.isQueryTotal()) {
            return paging;
        }

        List<AbstractFilter> highLightKeys = new ArrayList<>();
        List<Object> queryArgs = new ArrayList<>();
        String querySql = buildQuerySql(query, executor, queryArgs, highLightKeys);
        List<Map<String, Object>> data = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, queryArgs.toArray()));
        data = normalizeResultKeys(data, executor.getFields());
        replaceHighLight(highLightKeys, data);
        paging.setData(data);
        return paging;
    }

    @Override
    protected void delete(String sharding, Query query) {
        Executor executor = getExecutor(query.getType(), sharding);
        if (executor == null) {
            return;
        }
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(connector.buildWithQuotation(executor.getTable()));
        List<Object> params = new ArrayList<>();
        buildQuerySqlWithParams(query, params, sql, new ArrayList<>());
        final List<Object[]> args = new ArrayList<>();
        args.add(params.toArray());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql.toString(), args));
    }

    @Override
    protected void deleteAll(String sharding) {
        tables.computeIfPresent(sharding, (k, executor) -> {
            String sql = getExecutorSql(executor, k);
            executeSql(sql);
            if (!executor.systemTable) {
                return null;
            }
            return executor;
        });
    }

    @Override
    protected void batchInsert(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, true);
    }

    @Override
    protected void batchUpdate(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, false);
    }

    @Override
    protected void batchDelete(StorageEnum type, String sharding, List<String> ids) {
        final Executor executor = getExecutor(type, sharding);
        if (executor == null) {
            return;
        }
        final String sql = executor.getDelete();
        final List<Object[]> args = ids.stream().map(id -> new Object[] {id}).collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
    }

    @Override
    public void destroy() {
        if (connectorInstance != null) {
            connectorInstance.close();
        }
    }

    private void batchExecute(StorageEnum type, String sharding, List<Map> list, boolean insert) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        final Executor executor = getExecutor(type, sharding);
        if (executor == null) {
            return;
        }
        final String sql = insert ? executor.getInsert() : executor.getUpdate();
        final List<Object[]> args = list.stream()
                .map(row -> insert ? getInsertArgs(executor, row) : getUpdateArgs(executor, row))
                .collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
    }

    private Executor getExecutor(StorageEnum type, String sharding) {
        return tables.computeIfAbsent(sharding, table -> {
            Executor executor = tables.get(type.getType());
            if (executor == null) {
                throw new NullExecutorException("未知的存储类型");
            }
            Executor newExecutor = new Executor(executor.getType(), executor.getFields(), executor.isSystemTable(), executor.isOrderByUpdateTime());
            return createTableIfNotExist(table, newExecutor);
        });
    }

    private String getExecutorSql(Executor executor, String sharding) {
        return executor.isSystemTable() ? String.format(TRUNCATE_TABLE, PREFIX_TABLE.concat(sharding)) : String.format(DROP_TABLE, PREFIX_TABLE.concat(sharding));
    }

    private Object[] getInsertArgs(Executor executor, Map params) {
        return executor.getFields().stream().map(f -> params.get(f.getLabelName())).toArray();
    }

    private Object[] getUpdateArgs(Executor executor, Map params) {
        List<Object> args = new ArrayList<>();
        Object pk = null;
        for (Field f : executor.getFields()) {
            if (f.isPk()) {
                pk = params.get(f.getLabelName());
            }
            args.add(params.get(f.getLabelName()));
        }
        Assert.notNull(pk, "The primaryKey is null.");
        args.add(pk);
        return args.toArray();
    }

    private String buildQuerySql(Query query, Executor executor, List<Object> args, List<AbstractFilter> highLightKeys) {
        StringBuilder sql = new StringBuilder(buildSelectFromSql(query, executor));
        buildQuerySqlWithParams(query, args, sql, highLightKeys);
        sql.append(" order by ");
        if (query.hasCustomOrderBy()) {
            buildCustomOrderBy(query, sql);
        } else {
            buildDefaultOrderBy(query, executor, sql);
        }
        sql.append(DatabaseConstant.SQLITE_PAGE_SQL);
        args.add(query.getPageSize());
        args.add((query.getPageNum() - 1) * query.getPageSize());
        return sql.toString();
    }

    private String buildSelectFromSql(Query query, Executor executor) {
        if (!query.hasSelectField()) {
            return executor.getQuery();
        }
        Set<String> includeLabels = query.getSelectFlied();

        Database database = connector;
        List<String> selectedFields = new ArrayList<>();
        for (Field field : executor.getFields()) {
            String label = field.getLabelName();
            if (!CollectionUtils.isEmpty(includeLabels) && !includeLabels.contains(label)) {
                continue;
            }
            if (StringUtil.isNotBlank(label)) {
                selectedFields.add(database.buildWithQuotation(field.getName()) + " AS " + label);
            } else if (!database.buildCustom(selectedFields, field)) {
                selectedFields.add(database.buildWithQuotation(field.getName()));
            }
        }
        if (selectedFields.isEmpty()) {
            return executor.getQuery();
        }
        return String.format("SELECT %s FROM %s", StringUtil.join(selectedFields, StringUtil.COMMA), database.buildWithQuotation(executor.getTable()));
    }

    private void buildCustomOrderBy(Query query, StringBuilder sql) {
        List<Query.OrderBy> orderByList = query.getOrderByList();
        for (int i = 0; i < orderByList.size(); i++) {
            if (i > 0) {
                sql.append(StringUtil.COMMA);
            }
            Query.OrderBy orderBy = orderByList.get(i);
            sql.append(UnderlineToCamelUtils.camelToUnderline(orderBy.getFieldName()));
            sql.append(" ").append(orderBy.getSort() != null ? orderBy.getSort().getCode() : query.getSort().getCode());
        }
    }

    private void buildDefaultOrderBy(Query query, Executor executor, StringBuilder sql) {
        if (executor.isOrderByUpdateTime()) {
            sql.append(UnderlineToCamelUtils.camelToUnderline(ConfigConstant.CONFIG_MODEL_UPDATE_TIME)).append(StringUtil.COMMA);
        }
        sql.append(UnderlineToCamelUtils.camelToUnderline(ConfigConstant.CONFIG_MODEL_CREATE_TIME));
        sql.append(" ").append(query.getSort().getCode());
    }

    private String buildQueryCountSql(Query query, Executor executor, List<Object> args) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ").append(connector.buildWithQuotation(executor.getTable()));
        buildQuerySqlWithParams(query, args, sql, null);
        return sql.toString();
    }

    private void buildQuerySqlWithParams(Query query, List<Object> args, StringBuilder sql, List<AbstractFilter> highLightKeys) {
        BooleanFilter baseQuery = query.getBooleanFilter();
        List<BooleanFilter> clauses = baseQuery.getClauses();
        List<AbstractFilter> filters = baseQuery.getFilters();
        if (CollectionUtils.isEmpty(clauses) && CollectionUtils.isEmpty(filters)) {
            return;
        }

        sql.append(" WHERE ");
        if (!CollectionUtils.isEmpty(filters)) {
            buildQuerySqlWithFilters(filters, args, sql, highLightKeys);
            return;
        }
        buildQuerySqlWithBooleanFilters(clauses, args, sql, highLightKeys);
    }

    private void buildQuerySqlWithFilters(List<AbstractFilter> filters, List<Object> args, StringBuilder sql, List<AbstractFilter> highLightKeys) {
        int size = filters.size();
        for (int i = 0; i < size; i++) {
            AbstractFilter p = filters.get(i);
            if (i > 0) {
                sql.append(" ").append(p.getOperation().toUpperCase()).append(" ");
            }

            FilterEnum filterEnum = FilterEnum.getFilterEnum(p.getFilter());
            String name = UnderlineToCamelUtils.camelToUnderline(p.getName());
            sql.append(connector.buildWithQuotation(name));
            sql.append(String.format(" %s ?", filterEnum.getName()));
            switch (filterEnum) {
                case EQUAL:
                case NOT_EQUAL:
                case LT:
                case LT_AND_EQUAL:
                case GT:
                case GT_AND_EQUAL:
                    args.add(p.getValue());
                    break;
                case LIKE:
                    args.add(new StringBuilder("%").append(p.getValue()).append("%"));
                    break;
                case IN:
                    args.add(new StringBuilder("(").append(p.getValue()).append(")"));
                    break;
                case IS_NULL:
                case IS_NOT_NULL:
                    break;
                default:
                    throw new H2Exception("Unsupported filter type: " + filterEnum.getName());
            }
            if (null != highLightKeys && p.isEnableHighLightSearch()) {
                highLightKeys.add(p);
            }
        }
    }

    private void buildQuerySqlWithBooleanFilters(List<BooleanFilter> clauses, List<Object> args, StringBuilder sql, List<AbstractFilter> highLightKeys) {
        int size = clauses.size();
        for (int i = 0; i < size; i++) {
            BooleanFilter booleanFilter = clauses.get(i);
            List<AbstractFilter> filters = booleanFilter.getFilters();
            if (CollectionUtils.isEmpty(filters)) {
                continue;
            }

            if (i > 0) {
                sql.append(" ").append(booleanFilter.getOperationEnum().name().toUpperCase()).append(" ");
            }
            if (size > 0) {
                sql.append("(");
            }
            buildQuerySqlWithFilters(filters, args, sql, highLightKeys);
            if (size > 0) {
                sql.append(")");
            }
        }
    }

    private void initTable() {
        FieldBuilder builder = new FieldBuilder();
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> configFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> logFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.DATA_SUCCESS, ConfigConstant.DATA_TABLE_GROUP_ID, ConfigConstant.DATA_TARGET_TABLE_NAME, ConfigConstant.DATA_EVENT, ConfigConstant.DATA_ERROR, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.BINLOG_DATA);
        List<Field> dataFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.TASK_STATUS, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_JSON, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        List<Field> taskFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.TASK_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.TASK_STATUS, ConfigConstant.TASK_SOURCE_TABLE_NAME, ConfigConstant.DATA_TARGET_TABLE_NAME,
                ConfigConstant.TASK_SOURCE_TOTAL, ConfigConstant.TASK_TARGET_TOTAL, ConfigConstant.TASK_DIFF_TOTAL, ConfigConstant.TASK_FIXED_TOTAL,
                ConfigConstant.TASK_CONTENT, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        List<Field> dataVerifyDetailFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.TASK_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.TASK_STATUS,
                ConfigConstant.DATABASE_SYNC_DETAIL_TABLE_INDEX, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE,
                ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE,
                ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_SCHEMA,
                ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_TABLE, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_TABLE,
                ConfigConstant.TASK_SOURCE_TOTAL, ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL,
                ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, ConfigConstant.TASK_CONTENT,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        List<Field> databaseSyncDetailFields = builder.getFields();

        tables.computeIfAbsent(StorageEnum.CONFIG.getType(), k -> new Executor(k, configFields, true, true));
        tables.computeIfAbsent(StorageEnum.LOG.getType(), k -> new Executor(k, logFields, true, false));
        tables.computeIfAbsent(StorageEnum.DATA.getType(), k -> new Executor(k, dataFields, false, false));
        tables.computeIfAbsent(StorageEnum.TASK.getType(), k -> new Executor(k, taskFields, true, true));
        tables.computeIfAbsent(StorageEnum.VALIDATE_SYNC_DETAIL.getType(), k -> new Executor(k, dataVerifyDetailFields, true, true));
        tables.computeIfAbsent(StorageEnum.DATABASE_SYNC_DETAIL.getType(), k -> new Executor(k, databaseSyncDetailFields, true, true));
        tables.forEach((tableName, e) -> {
            if (e.isSystemTable()) {
                createTableIfNotExist(tableName, e);
            }
        });

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private Executor createTableIfNotExist(String table, Executor executor) {
        table = PREFIX_TABLE.concat(table);
        if (!tableExists(table)) {
            executeSql(buildCreateTableSql(table, executor.getFields()));
        }

        // 老版本升级：动态补齐新增字段/索引
        upgradeTableColumns(executor.getType(), table);

        List<Field> fields = executor.getFields();
        // 主键列名须与 Field.name 一致（如 ID），不可用 label（id），否则 H2 引号标识符大小写敏感
        final SqlBuilderConfig config = new SqlBuilderConfig(connector, "", table, buildPrimaryKeys(fields), fields, "");

        String query = SqlBuilderEnum.QUERY.getSqlBuilder().buildQuerySql(config);
        String insert = SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
        String update = SqlBuilderEnum.UPDATE.getSqlBuilder().buildSql(config);
        String delete = SqlBuilderEnum.DELETE.getSqlBuilder().buildSql(config);
        executor.setTable(table).setQuery(query).setInsert(insert).setUpdate(update).setDelete(delete);
        return executor;
    }

    /**
     * 老版本升级：按表类型动态补齐新增字段/索引（已存在则跳过）
     *
     * @param type  存储类型
     * @param table 已带前缀的表名
     */
    private void upgradeTableColumns(String type, String table) {
        if (StringUtil.isBlank(type)) {
            return;
        }
        if (StorageEnum.DATABASE_SYNC_DETAIL.getType().equals(type)) {
            addColumnIfNotExist(table, "STATUS", "INT DEFAULT 0 NOT NULL");
            addColumnIfNotExist(table, "TARGET_SCHEMA", "VARCHAR(512) DEFAULT ''");
            rebuildUniqueIndexIfNeeded(table, UK_DATABASE_SYNC_DETAIL, "TARGET_SCHEMA", UK_DATABASE_SYNC_DETAIL_COLUMNS);
            return;
        }
        if (StorageEnum.VALIDATE_SYNC_DETAIL.getType().equals(type)) {
            addColumnIfNotExist(table, "STATUS", "INT DEFAULT 0 NOT NULL");
        }
    }

    /**
     * 检查字段是否存在，不存在则动态新增
     *
     * @param table            已带前缀的表名
     * @param column           字段名
     * @param columnDefinition 字段类型定义（不含列名）
     */
    private void addColumnIfNotExist(String table, String column, String columnDefinition) {
        if (columnExists(table, column)) {
            return;
        }
        String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                connector.buildWithQuotation(table),
                connector.buildWithQuotation(column.toUpperCase()),
                columnDefinition);
        executeSql(alterSql);
    }

    /**
     * 唯一索引未包含指定列时，删除旧索引并按新列定义重建
     *
     * @param table          已带前缀的表名
     * @param indexName      唯一索引名
     * @param requiredColumn 索引必须包含的列
     * @param indexColumns   新唯一索引列定义
     */
    private void rebuildUniqueIndexIfNeeded(String table, String indexName, String requiredColumn, String indexColumns) {
        if (indexColumnExists(table, indexName, requiredColumn)) {
            return;
        }
        if (indexExists(table, indexName)) {
            executeSql(String.format("DROP INDEX IF EXISTS %s", connector.buildWithQuotation(indexName)));
        }
        executeSql(String.format("CREATE UNIQUE INDEX %s ON %s (%s)",
                connector.buildWithQuotation(indexName),
                connector.buildWithQuotation(table),
                indexColumns));
    }

    private List<String> buildPrimaryKeys(List<Field> fields) {
        List<String> primaryKeys = new ArrayList<>();
        for (Field field : fields) {
            if (field.isPk()) {
                primaryKeys.add(field.getName());
            }
        }
        return primaryKeys;
    }

    private boolean tableExists(String tableName) {
        Long count = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject(QUERY_TABLE_EXISTS, new Object[] {tableName}, Long.class));
        return count != null && count > 0;
    }

    private boolean columnExists(String tableName, String columnName) {
        Long count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(QUERY_COLUMN_EXISTS, new Object[] {tableName, columnName}, Long.class));
        return count != null && count > 0;
    }

    private boolean indexExists(String tableName, String indexName) {
        Long count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(QUERY_INDEX_EXISTS, new Object[] {tableName, indexName}, Long.class));
        return count != null && count > 0;
    }

    private boolean indexColumnExists(String tableName, String indexName, String columnName) {
        Long count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(QUERY_INDEX_COLUMN_EXISTS, new Object[] {tableName, indexName, columnName}, Long.class));
        return count != null && count > 0;
    }

    private String buildCreateTableSql(String table, List<Field> fields) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(connector.buildWithQuotation(table)).append(" (");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append(connector.buildWithQuotation(field.getName().toUpperCase())).append(" ").append(resolveType(field));
            if (field.isPk()) {
                ddl.append(" NOT NULL");
            }
        }
        ddl.append(", PRIMARY KEY (").append(connector.buildWithQuotation(ConfigConstant.CONFIG_MODEL_ID.toUpperCase())).append("))");
        return ddl.toString();
    }

    private String resolveType(Field field) {
        switch (field.getType()) {
            case Types.VARCHAR:
                return "VARCHAR(512)";
            case Types.LONGVARCHAR:
                return "CLOB";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.BLOB:
            case Types.VARBINARY:
                return "BLOB";
            default:
                return StringUtil.isNotBlank(field.getTypeName()) ? field.getTypeName() : "VARCHAR(512)";
        }
    }

    private void executeSql(String ddl) {
        connectorInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(ddl);
            logger.info(ddl);
            return true;
        });
    }

    private void replaceHighLight(List<AbstractFilter> highLightKeys, List<Map<String, Object>> list) {
        if (!CollectionUtils.isEmpty(list) && !CollectionUtils.isEmpty(highLightKeys)) {
            list.forEach(row -> highLightKeys.forEach(paramFilter -> {
                String text = String.valueOf(row.get(paramFilter.getName()));
                String replacement = "<span style='color:red'>" + paramFilter.getValue() + "</span>";
                row.put(paramFilter.getName(), StringUtil.replace(text, paramFilter.getValue(), replacement));
            }));
        }
    }

    /**
     * H2（MODE=MySQL）JDBC 返回的列名多为大写且无下划线
     */
    private List<Map<String, Object>> normalizeResultKeys(List<Map<String, Object>> data, List<Field> fields) {
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(fields)) {
            return data;
        }
        List<Map<String, Object>> normalized = new ArrayList<>(data.size());
        for (Map<String, Object> row : data) {
            Map<String, Object> newRow = new LinkedHashMap<>();
            row.forEach((key, value) -> newRow.put(resolveLabelName(key, fields), value));
            normalized.add(newRow);
        }
        return normalized;
    }

    private String resolveLabelName(Object key, List<Field> fields) {
        if (key == null) {
            return StringUtil.EMPTY;
        }
        String keyStr = String.valueOf(key);
        String compactKey = compactColumnKey(keyStr);
        for (Field field : fields) {
            if (compactKey.equals(compactColumnKey(field.getName()))) {
                return field.getLabelName();
            }
        }
        if (keyStr.contains(StringUtil.UNDERLINE)) {
            return UnderlineToCamelUtils.underlineToCamel(keyStr.toLowerCase(), true);
        }
        return keyStr;
    }

    private String compactColumnKey(String key) {
        return key.replace(StringUtil.UNDERLINE, StringUtil.EMPTY).toUpperCase();
    }

    static final class FieldBuilder {
        Map<String, Field> fieldMap;
        List<Field> fields;

        FieldBuilder() {
            fieldMap = Stream.of(new Field(ConfigConstant.CONFIG_MODEL_ID, "VARCHAR", Types.VARCHAR, true), new Field(ConfigConstant.CONFIG_MODEL_NAME, "VARCHAR", Types.VARCHAR), new Field(
                            ConfigConstant.CONFIG_MODEL_TYPE, "VARCHAR",
                            Types.VARCHAR), new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, "BIGINT", Types.BIGINT), new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, "BIGINT",
                            Types.BIGINT), new Field(ConfigConstant.CONFIG_MODEL_JSON, "LONGVARCHAR", Types.LONGVARCHAR), new Field(ConfigConstant.DATA_SUCCESS, "INTEGER",
                            Types.INTEGER), new Field(ConfigConstant.DATA_TABLE_GROUP_ID, "VARCHAR", Types.VARCHAR), new Field(ConfigConstant.DATA_TARGET_TABLE_NAME, "VARCHAR",
                            Types.VARCHAR), new Field(ConfigConstant.DATA_EVENT, "VARCHAR", Types.VARCHAR), new Field(ConfigConstant.DATA_ERROR, "LONGVARCHAR",
                            Types.LONGVARCHAR), new Field(ConfigConstant.BINLOG_DATA, "VARBINARY", Types.BLOB), new Field(ConfigConstant.TASK_ID, "VARCHAR",
                            Types.VARCHAR), new Field(ConfigConstant.TASK_STATUS, "INTEGER", Types.INTEGER), new Field(ConfigConstant.TASK_SOURCE_TABLE_NAME, "VARCHAR",
                            Types.VARCHAR), new Field(ConfigConstant.TASK_SOURCE_TOTAL, "BIGINT", Types.BIGINT), new Field(ConfigConstant.TASK_TARGET_TOTAL, "BIGINT", Types.BIGINT),
                    new Field(ConfigConstant.TASK_DIFF_TOTAL, "BIGINT", Types.BIGINT), new Field(ConfigConstant.TASK_FIXED_TOTAL, "BIGINT", Types.BIGINT),
                    new Field(ConfigConstant.TASK_CONTENT, "LONGVARCHAR", Types.LONGVARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_TABLE_INDEX, "INTEGER", Types.INTEGER),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_SCHEMA, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_TABLE, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_TABLE, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL, "BIGINT", Types.BIGINT),
                    new Field(ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, "BIGINT", Types.BIGINT))
                    .peek(field -> {
                        field.setLabelName(field.getName());
                        String columnName = UnderlineToCamelUtils.camelToUnderline(field.getName());
                        field.setName(columnName);
                    }).collect(Collectors.toMap(Field::getLabelName, field -> field));
        }

        List<Field> getFields() {
            return fields;
        }

        void build(String... fieldNames) {
            fields = new ArrayList<>(fieldNames.length);
            for (String fieldName : fieldNames) {
                Field field = fieldMap.get(fieldName);
                if (field != null) {
                    fields.add(field);
                }
            }
        }
    }

    static final class Executor {
        private String table;
        private String query;
        private String insert;
        private String update;
        private String delete;
        private final String type;
        private final List<Field> fields;
        private final boolean systemTable;
        private final boolean orderByUpdateTime;

        Executor(String type, List<Field> fields, boolean systemTable, boolean orderByUpdateTime) {
            this.type = type;
            this.fields = fields;
            this.systemTable = systemTable;
            this.orderByUpdateTime = orderByUpdateTime;
        }

        Executor setTable(String table) {
            this.table = table;
            return this;
        }

        String getTable() {
            return table;
        }

        String getQuery() {
            return query;
        }

        Executor setQuery(String query) {
            this.query = query;
            return this;
        }

        String getInsert() {
            return insert;
        }

        Executor setInsert(String insert) {
            this.insert = insert;
            return this;
        }

        String getUpdate() {
            return update;
        }

        Executor setUpdate(String update) {
            this.update = update;
            return this;
        }

        String getDelete() {
            return delete;
        }

        Executor setDelete(String delete) {
            this.delete = delete;
            return this;
        }

        String getType() {
            return type;
        }

        List<Field> getFields() {
            return fields;
        }

        boolean isSystemTable() {
            return systemTable;
        }

        boolean isOrderByUpdateTime() {
            return orderByUpdateTime;
        }
    }

}
