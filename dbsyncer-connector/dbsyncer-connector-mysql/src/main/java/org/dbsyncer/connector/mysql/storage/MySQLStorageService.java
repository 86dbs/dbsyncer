/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.storage;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UnderlineToCamelUtils;
import org.dbsyncer.connector.mysql.MySQLConnector;
import org.dbsyncer.connector.mysql.MySQLException;
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
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 将数据存储在mysql
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020-01-08 15:17
 */
public class MySQLStorageService extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String PREFIX_TABLE = "dbsyncer_";
    private final String SHOW_TABLE = "show tables where Tables_in_%s = '%s'";
    private final String DROP_TABLE = "DROP TABLE IF EXISTS %s";
    private final String TRUNCATE_TABLE = "TRUNCATE TABLE %s";
    private final String QUERY_INDEX_EXISTS ="SELECT COUNT(1) FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ?";
    private final MySQLConnector connector = new MySQLConnector();
    private final Map<String, Executor> tables = new ConcurrentHashMap<>();
    private DatabaseConnectorInstance connectorInstance;
    private String database;

    @Override
    public void init(Properties properties) {
        DatabaseConfig config = new DatabaseConfig();
        config.setConnectorType(properties.getProperty("dbsyncer.storage.type"));
        String url = properties.getProperty("dbsyncer.storage.mysql.url");
        String username = properties.getProperty("dbsyncer.storage.mysql.username", "admin");
        String password = properties.getProperty("dbsyncer.storage.mysql.password", "admin");
        config.setUsername(StringUtil.replace(username.trim(), "\t", StringUtil.EMPTY));
        config.setPassword(StringUtil.replace(password.trim(), "\t", StringUtil.EMPTY));
        config.setDriverClassName(properties.getProperty("dbsyncer.storage.mysql.driver-class-name"));
        config.setUrl(url);
        logger.info("url:{}", url);
        database = DatabaseUtil.getDatabaseName(url);
        connectorInstance = new DatabaseConnectorInstance(config);
        // 初始化表
        initTable();
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
            return CollectionUtils.isEmpty(data) ? new ArrayList<>() : data;
        } catch (Exception e) {
            if (isTableMissing(e)) {
                logger.debug("selectList skip missing table: {}", e.getMessage());
                return new ArrayList<>();
            }
            throw e instanceof RuntimeException ? (RuntimeException) e : new MySQLException(e.getMessage(), e);
        }
    }

    @Override
    protected List<Map<String, Object>> selectList(String sql, int pageNum, int pageSize, Object[] args) {
        // MySQL：LIMIT ?,? → offset, pageSize
        Object[] pageArgs = new Object[(args == null ? 0 : args.length) + 2];
        if (args != null && args.length > 0) {
            System.arraycopy(args, 0, pageArgs, 0, args.length);
        }
        pageArgs[pageArgs.length - 2] = (pageNum - 1) * pageSize;
        pageArgs[pageArgs.length - 1] = pageSize;
        return selectList(sql + DatabaseConstant.MYSQL_PAGE_SQL, pageArgs);
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
            replaceHighLight(highLightKeys, data);
            paging.setData(data);
            return paging;
        } catch (Exception e) {
            if (isTableMissing(e)) {
                tables.remove(sharding);
                logger.debug("select skip missing table, sharding={}: {}", sharding, e.getMessage());
                return paging;
            }
            throw e instanceof RuntimeException ? (RuntimeException) e : new MySQLException(e.getMessage(), e);
        }
    }

    @Override
    protected void delete(String sharding, Query query) {
        Executor executor = getExecutor(query.getType(), sharding, false);
        if (executor == null) {
            return;
        }
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(StringUtil.BACK_QUOTE).append(executor.getTable()).append(StringUtil.BACK_QUOTE);
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
        batchExecute(type, sharding, list, new ExecuteMapper() {

            @Override
            public String getSql(Executor executor) {
                return executor.getInsert();
            }

            @Override
            public Object[] getArgs(Executor executor, Map params) {
                return getInsertArgs(executor, params);
            }
        });
    }

    @Override
    protected void batchUpdate(StorageEnum type, String sharding, List<Map> list) {
        batchExecute(type, sharding, list, new ExecuteMapper() {

            @Override
            public String getSql(Executor executor) {
                return executor.getUpdate();
            }

            @Override
            public Object[] getArgs(Executor executor, Map params) {
                return getUpdateArgs(executor, params);
            }
        });
    }

    @Override
    protected void batchDelete(StorageEnum type, String sharding, List<String> ids) {
        final Executor executor = getExecutor(type, sharding, false);
        if (executor == null) {
            return;
        }
        final String sql = executor.getDelete();
        final List<Object[]> args = ids.stream().map(id -> new Object[]{id}).collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
    }

    @Override
    protected void batchIncrement(StorageEnum type, String sharding, String id, Map<String, Long> deltas) {
        if (CollectionUtils.isEmpty(deltas)) {
            return;
        }
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
            sql.append(quotation).append(" = ").append(quotation).append(" + ?");
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
        connectorInstance.close();
    }

    private void batchExecute(StorageEnum type, String sharding, List<Map> list, ExecuteMapper mapper) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        final Executor executor = getExecutor(type, sharding);
        if (executor == null) {
            return;
        }
        final String sql = mapper.getSql(executor);
        final List<Object[]> args = list.stream().map(row -> mapper.getArgs(executor, row)).collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
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

    private boolean tableExists(String tableName) {
        String sql = String.format(SHOW_TABLE, database, tableName);
        try {
            connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForMap(sql));
            return true;
        } catch (EmptyResultDataAccessException e) {
            return false;
        } catch (Exception e) {
            logger.debug("tableExists({}) failed: {}", tableName, e.getMessage());
            return false;
        }
    }

    private boolean isTableMissing(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("doesn't exist") || msg.contains("Unknown table") || msg.contains("not found"))) {
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
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        args.add((query.getPageNum() - 1) * query.getPageSize());
        args.add(query.getPageSize());
        return sql.toString();
    }

    /**
     * 可自定义 select 字段查询结果
     */
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
            // 自定义查询字段
            if (StringUtil.isNotBlank(label)) {
                selectedFields.add(database.buildWithQuotation(field.getName()) + " AS " + label);
            } else if (!database.buildCustom(selectedFields, field)) {
                selectedFields.add(database.buildWithQuotation(field.getName()));
            }
        }
        if (selectedFields.isEmpty()) {
            return executor.getQuery();
        }
        // 拼接最终SQL
        return String.format("SELECT %s FROM %s",
                StringUtil.join(selectedFields, StringUtil.COMMA),
                database.buildWithQuotation(executor.getTable()));
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
        StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM `").append(executor.getTable()).append("`");
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
        // 过滤值
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
                            throw new MySQLException("Unsupported filter type: " + filterEnum.getName());
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
            throw new MySQLException("IN filter values can not be empty.");
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
        // 解析查询
        int size = clauses.size();
        for (int i = 0; i < size; i++) {
            BooleanFilter booleanFilter = clauses.get(i);
            List<AbstractFilter> filters = booleanFilter.getFilters();
            if (CollectionUtils.isEmpty(filters)) {
                continue;
            }

            // 组合条件
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
        // 配置
        FieldBuilder builder = new FieldBuilder();
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> configFields = builder.getFields();

        // 用户配置：一行一用户，严格按 dbsyncer_user 拆分列
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.USER_USERNAME, ConfigConstant.USER_PASSWORD, ConfigConstant.USER_NICKNAME,
                ConfigConstant.USER_ROLE, ConfigConstant.USER_EMAIL, ConfigConstant.USER_PHONE);
        List<Field> userFields = builder.getFields();

        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
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
        // 创建表
        tables.forEach((tableName, e) -> {
            if (e.isSystemTable()) {
                createTableIfNotExist(tableName, e);
            }
        });

        // wait few seconds for execute sql
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private Executor createTableIfNotExist(String table, Executor executor) {
        table = PREFIX_TABLE.concat(table);
        // show tables where Tables_in_dbsyncer = "dbsyncer_config"
        String sql = String.format(SHOW_TABLE, database, table);
        try {
            connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForMap(sql));
        } catch (EmptyResultDataAccessException e) {
            // 不存在表
            String ddl = readSql(executor.getType(), executor.isSystemTable(), table);
            executeSql(ddl);
        }

        // 老版本升级：动态补齐新增字段
        upgradeTableColumns(executor.getType(), table);

        List<Field> fields = executor.getFields();
        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add(ConfigConstant.CONFIG_MODEL_ID);
        final SqlBuilderConfig config = new SqlBuilderConfig(connector, "", table, primaryKeys, fields, "");

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
        if (StorageEnum.TABLE_GROUP.getType().equals(type)) {
            createIndexIfNotExist(table, "IDX_TG_MAPPING",
                    "`TASK_ID`,`SOURCE_CONNECTOR_ID`,`TARGET_CONNECTOR_ID`,`SOURCE_DATABASE`,`TARGET_DATABASE`,`SOURCE_SCHEMA`,`TARGET_SCHEMA`,`SORT_INDEX`");
            return;
        }
        if (StorageEnum.TASK_DETAIL.getType().equals(type)) {
            // 新表 DDL 已含该索引；仅对老表补齐，存在则跳过，避免 Duplicate key name 打 ERROR
            createIndexIfNotExist(table, "IDX_TG_UPDATE", "`TABLE_GROUP_ID`,`UPDATE_TIME`");
        }
    }

    /**
     * 索引不存在则创建。
     */
    private void createIndexIfNotExist(String table, String indexName, String indexColumns) {
        if (indexExists(table, indexName)) {
            return;
        }
        try {
            executeSql(String.format("CREATE INDEX `%s` ON `%s` (%s)", indexName, table, indexColumns));
        } catch (Exception e) {
            logger.debug("skip create {} on {}: {}", indexName, table, e.getMessage());
        }
    }

    private boolean indexExists(String table, String indexName) {
        try {
            Long count = connectorInstance.execute(databaseTemplate ->
                    databaseTemplate.queryForObject(QUERY_INDEX_EXISTS,
                            new Object[]{database, table, indexName}, Long.class));
            return count != null && count > 0;
        } catch (Exception e) {
            logger.debug("indexExists({}, {}) failed: {}", table, indexName, e.getMessage());
            return false;
        }
    }

    private String readSql(String type, boolean systemTable, String table) {
        String filePath = getSqlFilePath(type);
        StringBuilder res = new StringBuilder();
        InputStream in = null;
        InputStreamReader isr = null;
        BufferedReader bf = null;
        try {
            in = this.getClass().getResourceAsStream(filePath);
            isr = new InputStreamReader(in, "UTF-8");
            bf = new BufferedReader(isr);
            String newLine;
            while ((newLine = bf.readLine()) != null) {
                res.append(newLine);
            }
        } catch (IOException e) {
            logger.error("failed read file:{}", filePath);
        } finally {
            IOUtils.closeQuietly(bf);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(in);
        }

        // 动态替换表名
        if (!systemTable) {
            String template = PREFIX_TABLE.concat(type);
            return StringUtil.replace(res.toString(), template, table);
        }
        return res.toString();
    }

    /**
     * 获取sql脚本路径
     *
     * @param type
     * @return /dbsyncer_mysql_config.sql
     */
    private String getSqlFilePath(String type) {
        return new StringBuilder(StringUtil.FORWARD_SLASH).append(PREFIX_TABLE).append(connector.getConnectorType().toLowerCase()).append(StringUtil.UNDERLINE).append(type).append(".sql").toString();
    }

    private void executeSql(String ddl) {
        connectorInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(ddl);
            logger.info(ddl);
            return true;
        });
    }

    private void replaceHighLight(List<AbstractFilter> highLightKeys, List<Map<String, Object>> list) {
        // 开启高亮
        if (!CollectionUtils.isEmpty(list) && !CollectionUtils.isEmpty(highLightKeys)) {
            list.forEach(row -> highLightKeys.forEach(paramFilter -> {
                String text = String.valueOf(row.get(paramFilter.getName()));
                String replacement = "<span style='color:red'>" + paramFilter.getValue() + "</span>";
                row.put(paramFilter.getName(), StringUtil.replace(text, paramFilter.getValue(), replacement));
            }));
        }
    }

    static final class FieldBuilder {

        Map<String, Field> fieldMap;
        List<Field> fields;

        public FieldBuilder() {
            fieldMap = Stream.of(new Field(ConfigConstant.CONFIG_MODEL_ID, "VARCHAR", Types.VARCHAR, true),
                            new Field(ConfigConstant.CONFIG_MODEL_NAME, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.CONFIG_MODEL_TYPE, "VARCHAR", Types.VARCHAR),
                            new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, "BIGINT", Types.BIGINT),
                            new Field(ConfigConstant.CONFIG_MODEL_JSON, "LONGVARCHAR", Types.LONGVARCHAR),
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
                        // 转换列下划线
                        String labelName = UnderlineToCamelUtils.camelToUnderline(field.getName());
                        field.setName(labelName);
                    }).collect(Collectors.toMap(Field::getLabelName, field -> field, (a, b) -> a));
        }

        public List<Field> getFields() {
            return fields;
        }

        public void build(String... fieldNames) {
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

        public Executor(String type, List<Field> fields, boolean systemTable, boolean orderByUpdateTime) {
            this.type = type;
            this.fields = fields;
            this.systemTable = systemTable;
            this.orderByUpdateTime = orderByUpdateTime;
        }

        public Executor setTable(String table) {
            this.table = table;
            return this;
        }

        public String getTable() {
            return table;
        }

        public String getQuery() {
            return query;
        }

        public Executor setQuery(String query) {
            this.query = query;
            return this;
        }

        public String getInsert() {
            return insert;
        }

        public Executor setInsert(String insert) {
            this.insert = insert;
            return this;
        }

        public String getUpdate() {
            return update;
        }

        public Executor setUpdate(String update) {
            this.update = update;
            return this;
        }

        public String getDelete() {
            return delete;
        }

        public Executor setDelete(String delete) {
            this.delete = delete;
            return this;
        }

        public String getType() {
            return type;
        }

        public List<Field> getFields() {
            return fields;
        }

        public boolean isSystemTable() {
            return systemTable;
        }

        public boolean isOrderByUpdateTime() {
            return orderByUpdateTime;
        }

    }

    interface ExecuteMapper {

        String getSql(Executor executor);

        Object[] getArgs(Executor executor, Map params);
    }
}