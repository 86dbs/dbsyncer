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
import org.dbsyncer.sdk.filter.impl.InFilter;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.storage.AbstractStorageService;
import org.dbsyncer.sdk.storage.migrate.StorageDataMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.LinkedCaseInsensitiveMap;

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
    private final String DROP_TABLE = "DROP TABLE IF EXISTS %s";
    private final String TRUNCATE_TABLE = "TRUNCATE TABLE %s";
    private final String QUERY_TABLE_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER(?)";
    private final String QUERY_INDEX_EXISTS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.INDEXES WHERE UPPER(TABLE_NAME) = UPPER(?) AND UPPER(INDEX_NAME) = UPPER(?)";

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
    protected List<Map<String, Object>> selectList(String sql, Object[] args) {
        try {
            List<Map<String, Object>> data = connectorInstance.execute(
                    databaseTemplate -> databaseTemplate.queryForList(sql, args));
            if (CollectionUtils.isEmpty(data)) {
                return new ArrayList<>();
            }
            // H2 列名常为大写/折叠大小写：下划线转小驼峰；其余保留原 key，并用大小写不敏感 Map，
            // 保证 AS sourceTable / SOURCETABLE 都能按 camelCase 读取（与结构化查询 normalizeResultKeys 语义对齐）
            List<Map<String, Object>> normalized = new ArrayList<>(data.size());
            for (Map<String, Object> row : data) {
                Map<String, Object> newRow = new LinkedCaseInsensitiveMap<>();
                row.forEach((key, value) -> {
                    String keyStr = key == null ? StringUtil.EMPTY : String.valueOf(key);
                    if (keyStr.contains(StringUtil.UNDERLINE)) {
                        newRow.put(UnderlineToCamelUtils.underlineToCamel(keyStr.toLowerCase(), true), value);
                    } else {
                        newRow.put(keyStr, value);
                    }
                });
                normalized.add(newRow);
            }
            return normalized;
        } catch (Exception e) {
            if (isTableMissing(e)) {
                logger.debug("selectList skip missing table: {}", e.getMessage());
                return new ArrayList<>();
            }
            throw new H2Exception(e);
        }
    }

    @Override
    protected List<Map<String, Object>> selectList(String sql, int pageNum, int pageSize, Object[] args) {
        // H2：limit ? OFFSET ?
        Object[] pageArgs = new Object[(args == null ? 0 : args.length) + 2];
        if (args != null && args.length > 0) {
            System.arraycopy(args, 0, pageArgs, 0, args.length);
        }
        pageArgs[pageArgs.length - 2] = pageSize;
        pageArgs[pageArgs.length - 1] = (pageNum - 1) * pageSize;
        return selectList(sql + DatabaseConstant.SQLITE_PAGE_SQL, pageArgs);
    }

    @Override
    protected int update(String sql, Object[] args) {
        try {
            Integer rows = connectorInstance.execute(databaseTemplate -> databaseTemplate.update(sql, args));
            return rows == null ? 0 : rows;
        } catch (Exception e) {
            if (isTableMissing(e)) {
                logger.debug("update skip missing table: {}", e.getMessage());
                return 0;
            }
            throw new H2Exception(e);
        }
    }

    @Override
    protected Paging select(String sharding, Query query) {
        Paging paging = new Paging(query.getPageNum(), query.getPageSize());
        // 读路径：分表不存在时不建表，避免错误 shardId 产生孤儿表
        Executor executor = getExecutor(query.getType(), sharding, false);
        if (executor == null) {
            return paging;
        }
        try {
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
        } catch (Exception e) {
            if (isTableMissing(e)) {
                tables.remove(sharding);
                logger.debug("select skip missing table, sharding={}: {}", sharding, e.getMessage());
                return paging;
            }
            throw e instanceof RuntimeException ? (RuntimeException) e : new H2Exception(e);
        }
    }

    @Override
    protected void delete(String sharding, Query query) {
        Executor executor = getExecutor(query.getType(), sharding, false);
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
        Executor executor = tables.remove(sharding);
        // 系统表：截断后放回缓存
        if (executor != null && executor.isSystemTable()) {
            tables.put(sharding, executor);
            executeSql(String.format(TRUNCATE_TABLE, PREFIX_TABLE.concat(sharding)));
            return;
        }
        // 动态分表：无论是否在内存缓存，都尝试 DROP，避免删任务后孤儿表残留
        String tableName = PREFIX_TABLE.concat(sharding);
        try {
            executeSql(String.format(DROP_TABLE, tableName));
        } catch (Exception e) {
            logger.debug("drop table {} skipped: {}", tableName, e.getMessage());
        }
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
        final Executor executor = getExecutor(type, sharding, false);
        if (executor == null) {
            return;
        }
        final String sql = executor.getDelete();
        final List<Object[]> args = ids.stream().map(id -> new Object[] {id}).collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
    }

    @Override
    protected void batchIncrement(StorageEnum type, String sharding, String id, Map<String, Long> deltas) {
        final Executor executor = getExecutor(type, sharding);
        if (executor == null) {
            return;
        }
        StringBuilder sql = new StringBuilder("UPDATE ").append(connector.buildWithQuotation(executor.getTable())).append(" SET ");
        List<Object> args = new ArrayList<>();
        boolean hasColumn = false;
        for (Map.Entry<String, Long> entry : deltas.entrySet()) {
            String column = resolveColumn(executor, entry.getKey());
            if (column == null || entry.getValue() == null) {
                continue;
            }
            if (hasColumn) {
                sql.append(", ");
            }
            String quotation = connector.buildWithQuotation(column);
            // 原子加减，结果小于 0 时钳为 0
            sql.append(quotation).append(" = GREATEST(").append(quotation).append(" + ?, 0)");
            args.add(entry.getValue());
            hasColumn = true;
        }
        if (!hasColumn) {
            return;
        }
        String updateTimeColumn = resolveColumn(executor, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        if (updateTimeColumn != null) {
            sql.append(", ").append(connector.buildWithQuotation(updateTimeColumn)).append(" = ?");
            args.add(System.currentTimeMillis());
        }
        sql.append(" WHERE ").append(connector.buildWithQuotation(ConfigConstant.CONFIG_MODEL_ID.toUpperCase())).append(" = ?");
        args.add(id);
        final List<Object[]> batchArgs = new ArrayList<>();
        batchArgs.add(args.toArray());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql.toString(), batchArgs));
    }

    private String resolveColumn(Executor executor, String labelName) {
        for (Field field : executor.getFields()) {
            if (field.getLabelName().equals(labelName)) {
                return field.getName().toUpperCase();
            }
        }
        return null;
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

    @Override
    protected void ensureShard(StorageEnum type, String sharding) {
        getExecutor(type, sharding, true);
    }

    private Executor getExecutor(StorageEnum type, String sharding) {
        return getExecutor(type, sharding, true);
    }

    /**
     * @param createIfAbsent true：写路径，分表不存在则建表；false：读/删路径，不存在则返回 null，避免错误 shard 产生孤儿表
     */
    private Executor getExecutor(StorageEnum type, String sharding, boolean createIfAbsent) {
        Executor template = tables.get(type.getType());
        if (template == null) {
            throw new NullExecutorException("未知的存储类型");
        }
        String physicalTable = PREFIX_TABLE.concat(sharding);
        // 读路径：缓存命中也要校验物理表，避免 DROP 后仍查已失效的 Executor
        if (!createIfAbsent && !template.isSystemTable()) {
            if (!tableExists(physicalTable)) {
                tables.remove(sharding);
                return null;
            }
        }
        Executor cached = tables.get(sharding);
        if (cached != null) {
            return cached;
        }
        return tables.computeIfAbsent(sharding, table -> {
            Executor newExecutor = new Executor(template.getType(), template.getFields(), template.isSystemTable(), template.isOrderByUpdateTime());
            return createTableIfNotExist(table, newExecutor);
        });
    }

    private boolean isTableMissing(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg == null) {
                continue;
            }
            String lower = msg.toLowerCase();
            if (lower.contains("doesn't exist") || lower.contains("not found") || lower.contains("unknown table")) {
                return true;
            }
        }
        return false;
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

            String name = UnderlineToCamelUtils.camelToUnderline(p.getName());
            sql.append(connector.buildWithQuotation(name));
            if (p instanceof InFilter) {
                appendInClause(p, args, sql);
            } else {
                FilterEnum filterEnum = FilterEnum.getFilterEnum(p.getFilter());
                if (filterEnum == FilterEnum.IN) {
                    appendInClause(p, args, sql);
                } else if (filterEnum == FilterEnum.IS_NULL || filterEnum == FilterEnum.IS_NOT_NULL) {
                    sql.append(" ").append(filterEnum.getName());
                } else {
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
                        default:
                            throw new H2Exception("Unsupported filter type: " + filterEnum.getName());
                    }
                }
            }
            if (null != highLightKeys && p.isEnableHighLightSearch()) {
                highLightKeys.add(p);
            }
        }
    }

    /**
     * 展开 IN：生成 {@code IN (?,?,?)} 并逐个绑定参数（逗号拼接值或 {@link InFilter#getBindValues()}）。
     */
    private void appendInClause(AbstractFilter filter, List<Object> args, StringBuilder sql) {
        List<Object> binds;
        if (filter instanceof InFilter) {
            binds = ((InFilter) filter).getBindValues();
        } else {
            String raw = filter.getValue() == null ? StringUtil.EMPTY : String.valueOf(filter.getValue());
            String[] parts = StringUtil.split(raw, StringUtil.COMMA);
            binds = new ArrayList<>();
            if (parts != null) {
                for (String part : parts) {
                    if (StringUtil.isNotBlank(part)) {
                        binds.add(part.trim());
                    }
                }
            }
        }
        if (CollectionUtils.isEmpty(binds)) {
            throw new H2Exception("IN filter values can not be empty.");
        }
        sql.append(" IN (");
        for (int j = 0; j < binds.size(); j++) {
            if (j > 0) {
                sql.append(StringUtil.COMMA);
            }
            sql.append("?");
        }
        sql.append(")");
        args.addAll(binds);
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

        // 用户配置：一行一用户，严格按 dbsyncer_user 拆分列
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.USER_USERNAME, ConfigConstant.USER_PASSWORD, ConfigConstant.USER_NICKNAME,
                ConfigConstant.USER_ROLE, ConfigConstant.USER_EMAIL, ConfigConstant.USER_PHONE);
        List<Field> userFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.CONNECTOR_IS_SOURCE, ConfigConstant.CONNECTOR_IS_TARGET, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> connectorFields = builder.getFields();

        // 表映射关系：关联信息拆分列 + json(字段映射/command等)
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.TABLE_GROUP_TASK_ID, ConfigConstant.TABLE_GROUP_SORT_INDEX,
                ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID, ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID,
                ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, ConfigConstant.TABLE_GROUP_TARGET_DATABASE,
                ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, ConfigConstant.TABLE_GROUP_TARGET_SCHEMA,
                ConfigConstant.TABLE_GROUP_SOURCE_TABLE, ConfigConstant.TABLE_GROUP_TARGET_TABLE,
                ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, ConfigConstant.TABLE_GROUP_TARGET_TOTAL,
                ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> tableGroupFields = builder.getFields();

        // 任务执行结果：严格按 dbsyncer_meta 拆分列(无 name/type/json)
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.META_TASK_ID, ConfigConstant.META_STATE, ConfigConstant.META_IS_TASK_DETAIL,
                ConfigConstant.META_TOTAL, ConfigConstant.META_SUCCESS, ConfigConstant.META_FAIL,
                ConfigConstant.META_DIFF, ConfigConstant.META_FIXED, ConfigConstant.META_SNAPSHOT);
        List<Field> metaFields = builder.getFields();

        // 任务执行明细：按任务分表(无 TASK_ID 列，靠 TABLE_GROUP_ID 关联)
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.DATA_TABLE_GROUP_ID, ConfigConstant.CONFIG_MODEL_TYPE,
                ConfigConstant.DETAIL_TARGET_TABLE, ConfigConstant.DETAIL_IS_SUCCESS, ConfigConstant.DATA_ERROR,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.BINLOG_DATA);
        List<Field> taskDetailFields = builder.getFields();

        // 日志
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> logFields = builder.getFields();

        // 任务配置表(同步/校验/迁移统一)：ID/NAME/TYPE/JSON/时间
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE,
                ConfigConstant.CONFIG_MODEL_JSON, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        List<Field> taskFields = builder.getFields();

        tables.computeIfAbsent(StorageEnum.CONFIG.getType(), k -> new Executor(k, configFields, true, true));
        tables.computeIfAbsent(StorageEnum.USER.getType(), k -> new Executor(k, userFields, true, true));
        tables.computeIfAbsent(StorageEnum.CONNECTOR.getType(), k -> new Executor(k, connectorFields, true, true));
        tables.computeIfAbsent(StorageEnum.TABLE_GROUP.getType(), k -> new Executor(k, tableGroupFields, true, true));
        tables.computeIfAbsent(StorageEnum.META.getType(), k -> new Executor(k, metaFields, true, true));
        tables.computeIfAbsent(StorageEnum.TASK_DETAIL.getType(), k -> new Executor(k, taskDetailFields, false, false));
        tables.computeIfAbsent(StorageEnum.LOG.getType(), k -> new Executor(k, logFields, true, false));
        tables.computeIfAbsent(StorageEnum.TASK.getType(), k -> new Executor(k, taskFields, true, true));

        // 建表前：新拆表齐全且 task 无 STATUS → 新版本跳过数据升级
        boolean newStorageSchema = isNewStorageSchema();
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

        // 兼容升级（临时脚本，若干版本后可删）
        StorageDataMigrator migrator = new H2StorageDataMigrator(this, connectorInstance);
        if (!newStorageSchema) {
            logger.info("未检测到完整新存储拆表（或仍含 task.STATUS），执行兼容升级");
        }
        migrator.run();
        dropTaskStatusColumnIfPresent();
    }

    /**
     * 是否已是新版本存储：关键拆表齐全，且 dbsyncer_task 无旧 STATUS 列。
     */
    private boolean isNewStorageSchema() {
        String taskTable = PREFIX_TABLE.concat(StorageEnum.TASK.getType());
        return tableExists(PREFIX_TABLE.concat(StorageEnum.USER.getType()))
                && tableExists(PREFIX_TABLE.concat(StorageEnum.CONNECTOR.getType()))
                && tableExists(taskTable)
                && tableExists(PREFIX_TABLE.concat(StorageEnum.TABLE_GROUP.getType()))
                && tableExists(PREFIX_TABLE.concat(StorageEnum.META.getType()))
                && !columnExists(taskTable, "STATUS");
    }

    private boolean columnExists(String tableName, String columnName) {
        if (StringUtil.isBlank(tableName) || StringUtil.isBlank(columnName)) {
            return false;
        }
        try {
            String sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = UPPER(?) AND UPPER(COLUMN_NAME) = UPPER(?)";
            Long cnt = connectorInstance.execute(tpl -> tpl.queryForObject(sql, new Object[]{tableName, columnName}, Long.class));
            return cnt != null && cnt > 0;
        } catch (Exception e) {
            logger.debug("columnExists({}.{}) failed: {}", tableName, columnName, e.getMessage());
            return false;
        }
    }

    /**
     * 删除旧 dbsyncer_task.STATUS 及依赖索引（与当前 DDL 对齐）。
     */
    private void dropTaskStatusColumnIfPresent() {
        String table = PREFIX_TABLE.concat(StorageEnum.TASK.getType());
        if (!tableExists(table) || !columnExists(table, "STATUS")) {
            return;
        }
        dropIndexIfExists(table, "IDX_TYPE_UPDATE_CREATE_TIME");
        dropIndexIfExists(table, "IDX_STATUS_UPDATE_TIME");
        try {
            executeSql(String.format("ALTER TABLE %s DROP COLUMN %s",
                    connector.buildWithQuotation(table),
                    connector.buildWithQuotation("STATUS")));
            logger.info("已删除 {}.STATUS", table);
        } catch (Exception e) {
            logger.warn("删除 {}.STATUS 失败: {}", table, e.getMessage());
        }
    }

    private void dropIndexIfExists(String table, String indexName) {
        if (!indexExists(table, indexName)) {
            return;
        }
        try {
            executeSql(String.format("DROP INDEX IF EXISTS %s", connector.buildWithQuotation(indexName)));
        } catch (Exception e) {
            logger.debug("drop index {} on {} skip: {}", indexName, table, e.getMessage());
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
        if (StorageEnum.CONNECTOR.getType().equals(type)) {
            addColumnIfNotExist(table, "IS_SOURCE", "INT NOT NULL DEFAULT 1");
            addColumnIfNotExist(table, "IS_TARGET", "INT NOT NULL DEFAULT 1");
            return;
        }
        if (StorageEnum.TABLE_GROUP.getType().equals(type)) {
            createIndexIfNotExist(table, "IDX_TASK_SORT", "`TASK_ID`,`SORT_INDEX`");
            createIndexIfNotExist(table, "IDX_TG_MAPPING",
                    "`TASK_ID`,`SOURCE_CONNECTOR_ID`,`TARGET_CONNECTOR_ID`,`SOURCE_DATABASE`,`TARGET_DATABASE`,`SOURCE_SCHEMA`,`TARGET_SCHEMA`,`SORT_INDEX`");
            return;
        }
        if (StorageEnum.TASK_DETAIL.getType().equals(type)) {
            // H2 索引名全局唯一：按表名哈希后缀，避免多分表冲突
            String suffix = Integer.toHexString(table.hashCode() & 0xffff);
            createIndexIfNotExist(table, "IDX_TG_UPD_" + suffix, "`TABLE_GROUP_ID`,`UPDATE_TIME`");
            return;
        }
        // 任务执行明细按任务分表(每表数据量有限)，H2 索引名为 schema 全局唯一，分表间不再单独建二级索引
        if (StorageEnum.META.getType().equals(type)) {
            createIndexIfNotExist(table, "IDX_STATE_UPDATE_TIME", "`STATE`,`UPDATE_TIME`");
            // 对齐 MySQL：按 TASK_ID + IS_TASK_DETAIL 查询/重置明细 Meta
            createIndexIfNotExist(table, "IDX_TASK_IS_DETAIL", "`TASK_ID`,`IS_TASK_DETAIL`");
            return;
        }
        if (StorageEnum.TASK.getType().equals(type)) {
            // STATUS 列须在数据迁移后删除，见 dropTaskStatusColumnIfPresent()
            createIndexIfNotExist(table, "IDX_UPDATE_TIME", "`UPDATE_TIME`");
            createIndexIfNotExist(table, "IDX_TYPE_UPDATE_TIME", "`TYPE`,`UPDATE_TIME`");
        }
    }

    /**
     * 列不存在则追加。
     */
    private void addColumnIfNotExist(String table, String columnName, String columnDef) {
        if (columnExists(table, columnName)) {
            return;
        }
        try {
            executeSql(String.format("ALTER TABLE %s ADD COLUMN %s %s",
                    connector.buildWithQuotation(table),
                    connector.buildWithQuotation(columnName),
                    columnDef));
            logger.info("已补齐 {}.{}", table, columnName);
        } catch (Exception e) {
            logger.warn("补齐 {}.{} 失败: {}", table, columnName, e.getMessage());
        }
    }

    /**
     * 普通二级索引不存在则创建
     *
     * @param table        已带前缀的表名
     * @param indexName    索引名
     * @param indexColumns 索引列定义
     */
    private void createIndexIfNotExist(String table, String indexName, String indexColumns) {
        if (indexExists(table, indexName)) {
            return;
        }
        executeSql(String.format("CREATE INDEX %s ON %s (%s)",
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
        try {
            Long count = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject(QUERY_TABLE_EXISTS, new Object[] {tableName}, Long.class));
            return count != null && count > 0;
        } catch (Exception e) {
            logger.debug("tableExists({}) failed: {}", tableName, e.getMessage());
            return false;
        }
    }

    private boolean indexExists(String tableName, String indexName) {
        Long count = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject(QUERY_INDEX_EXISTS, new Object[] {tableName, indexName}, Long.class));
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
            fieldMap = Stream.of(new Field(ConfigConstant.CONFIG_MODEL_ID, "VARCHAR", Types.VARCHAR, true),
                            new Field(ConfigConstant.CONFIG_MODEL_NAME, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.CONFIG_MODEL_TYPE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.CONFIG_MODEL_JSON, "LONGVARCHAR", Types.LONGVARCHAR),
                            new Field(ConfigConstant.CONNECTOR_IS_SOURCE, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.CONNECTOR_IS_TARGET, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.DATA_TABLE_GROUP_ID, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.DATA_ERROR, "LONGVARCHAR", Types.LONGVARCHAR),
                            new Field(ConfigConstant.BINLOG_DATA, "VARBINARY", Types.BLOB),
                            new Field(ConfigConstant.TASK_ID, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.DETAIL_IS_SUCCESS, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.DETAIL_TARGET_TABLE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.META_STATE, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.META_IS_TASK_DETAIL, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.META_TOTAL, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.META_SUCCESS, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.META_FAIL, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.META_DIFF, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.META_FIXED, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.META_SNAPSHOT, "LONGVARCHAR", Types.LONGVARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_SORT_INDEX, "INTEGER", Types.INTEGER),
                            new Field(ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_TARGET_DATABASE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_TARGET_TABLE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.TABLE_GROUP_TARGET_TOTAL, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.USER_USERNAME, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.USER_PASSWORD, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.USER_NICKNAME, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.USER_ROLE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.USER_EMAIL, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.USER_PHONE, "VARCHAR", Types.VARCHAR))
                    .peek(field -> {
                        field.setLabelName(field.getName());
                        String columnName = UnderlineToCamelUtils.camelToUnderline(field.getName());
                        field.setName(columnName);
                    }).collect(Collectors.toMap(Field::getLabelName, field -> field, (a, b) -> a));
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
