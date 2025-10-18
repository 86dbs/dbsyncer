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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 将数据存储在mysql
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public class MySQLStorageService extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String PREFIX_TABLE = "dbsyncer_";
    private final String SHOW_TABLE = "show tables where Tables_in_%s = '%s'";
    private final String DROP_TABLE = "DROP TABLE %s";
    private final String TRUNCATE_TABLE = "TRUNCATE TABLE %s";
    private final MySQLConnector connector = new MySQLConnector();
    private final Map<String, Executor> tables = new ConcurrentHashMap<>();
    private DatabaseConnectorInstance connectorInstance;
    private String database;

    @Override
    public void init(Properties properties) {
        DatabaseConfig config = new DatabaseConfig();
        config.setConnectorType(properties.getProperty("dbsyncer.storage.type"));
        config.setUrl(properties.getProperty("dbsyncer.storage.mysql.url", "jdbc:mysql://127.0.0.1:3306/dbsyncer?rewriteBatchedStatements=true&seUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&tinyInt1isBit=false&allowPublicKeyRetrieval=true"));
        String username = properties.getProperty("dbsyncer.storage.mysql.username", "admin");
        String password = properties.getProperty("dbsyncer.storage.mysql.password", "admin");
        config.setUsername(StringUtil.replace(username.trim(), "\t", StringUtil.EMPTY));
        config.setPassword(StringUtil.replace(password.trim(), "\t", StringUtil.EMPTY));
        config.setDriverClassName(properties.getProperty("dbsyncer.storage.mysql.driver-class-name"));
        logger.info("url:{}", config.getUrl());
        database = DatabaseUtil.getDatabaseName(config.getUrl());
        connectorInstance = new DatabaseConnectorInstance(config);
        // 初始化表
        initTable();
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
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(StringUtil.BACK_QUOTE).append(executor.getTable()).append(StringUtil.BACK_QUOTE);
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
            // 非系统表
            if (!executor.systemTable) {
                return null;
            }
            return executor;
        });
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
        final Executor executor = getExecutor(type, sharding);
        if (executor == null) {
            return;
        }
        final String sql = executor.getDelete();
        final List<Object[]> args = ids.stream().map(id -> new Object[]{id}).collect(Collectors.toList());
        connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, args));
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
                continue;
            }
            args.add(params.get(f.getLabelName()));
        }

        Assert.notNull(pk, "The primaryKey is null.");
        args.add(pk);
        return args.toArray();
    }

    private String buildQuerySql(Query query, Executor executor, List<Object> args, List<AbstractFilter> highLightKeys) {
        StringBuilder sql = new StringBuilder(executor.getQuery());
        buildQuerySqlWithParams(query, args, sql, highLightKeys);
        // order by updateTime,createTime desc
        sql.append(" order by ");
        if (executor.isOrderByUpdateTime()) {
            sql.append(UnderlineToCamelUtils.camelToUnderline(ConfigConstant.CONFIG_MODEL_UPDATE_TIME)).append(StringUtil.COMMA);
        }
        sql.append(UnderlineToCamelUtils.camelToUnderline(ConfigConstant.CONFIG_MODEL_CREATE_TIME));
        sql.append(" ").append(query.getSort().getCode());
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        args.add((query.getPageNum() - 1) * query.getPageSize());
        args.add(query.getPageSize());
        return sql.toString();
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

        String quotation = connector.buildSqlWithQuotation();
        for (int i = 0; i < size; i++) {
            AbstractFilter p = filters.get(i);

            if (i > 0) {
                sql.append(" ").append(p.getOperation().toUpperCase()).append(" ");
            }

            FilterEnum filterEnum = FilterEnum.getFilterEnum(p.getFilter());
            String name = UnderlineToCamelUtils.camelToUnderline(p.getName());
            switch (filterEnum) {
                case EQUAL:
                case NOT_EQUAL:
                case LT:
                case LT_AND_EQUAL:
                case GT:
                case GT_AND_EQUAL:
                    sql.append(quotation).append(name).append(quotation).append(String.format(" %s ?", filterEnum.getName()));
                    args.add(p.getValue());
                    break;
                case LIKE:
                    sql.append(quotation).append(name).append(quotation).append(String.format(" %s ?", filterEnum.getName()));
                    args.add(new StringBuilder("%").append(p.getValue()).append("%"));
                    break;
                case IN:
                    sql.append(quotation).append(name).append(quotation).append(String.format(" %s ?", filterEnum.getName()));
                    args.add(new StringBuilder("(").append(p.getValue()).append(")"));
                    break;
                case IS_NULL:
                case IS_NOT_NULL:
                    sql.append(quotation).append(name).append(quotation).append(String.format(" %s ", filterEnum.getName()));
                    break;
                default:
                    throw new MySQLException("Unsupported filter type: " + filterEnum.getName());
            }
            if (null != highLightKeys && p.isEnableHighLightSearch()) {
                highLightKeys.add(p);
            }
        }
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

        // 日志
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> logFields = builder.getFields();

        // 数据
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.DATA_SUCCESS, ConfigConstant.DATA_TABLE_GROUP_ID, ConfigConstant.DATA_TARGET_TABLE_NAME, ConfigConstant.DATA_EVENT, ConfigConstant.DATA_ERROR, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.BINLOG_DATA);
        List<Field> dataFields = builder.getFields();

        // 任务
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.TASK_STATUS, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<Field> taskFields = builder.getFields();

        //任务详情
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.TASK_ID, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.TARGET_TABLE_NAME, ConfigConstant.SOURCE_TABLE_NAME, ConfigConstant.CONTENT);
        List<Field> taskDetailFields = builder.getFields();

        tables.computeIfAbsent(StorageEnum.CONFIG.getType(), k -> new Executor(k, configFields, true, true));
        tables.computeIfAbsent(StorageEnum.LOG.getType(), k -> new Executor(k, logFields, true, false));
        tables.computeIfAbsent(StorageEnum.DATA.getType(), k -> new Executor(k, dataFields, false, false));
        tables.computeIfAbsent(StorageEnum.TASK.getType(), k -> new Executor(k, taskFields, true, true));
        tables.computeIfAbsent(StorageEnum.TASK_DATA_VERIFICATION_DETAIL.getType(), k -> new Executor(k, taskDetailFields, true, true));
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
            list.forEach(row ->
                    highLightKeys.forEach(paramFilter -> {
                        String text = String.valueOf(row.get(paramFilter.getName()));
                        String replacement = "<span style='color:red'>" + paramFilter.getValue() + "</span>";
                        row.put(paramFilter.getName(), StringUtil.replace(text, paramFilter.getValue(), replacement));
                    })
            );
        }
    }

    static final class FieldBuilder {
        Map<String, Field> fieldMap;
        List<Field> fields;

        public FieldBuilder() {
            fieldMap = Stream.of(
                    new Field(ConfigConstant.CONFIG_MODEL_ID, "VARCHAR", Types.VARCHAR, true),
                    new Field(ConfigConstant.CONFIG_MODEL_NAME, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.CONFIG_MODEL_TYPE, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, "BIGINT", Types.BIGINT),
                    new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, "BIGINT", Types.BIGINT),
                    new Field(ConfigConstant.CONFIG_MODEL_JSON, "LONGVARCHAR", Types.LONGVARCHAR),
                    new Field(ConfigConstant.DATA_SUCCESS, "INTEGER", Types.INTEGER),
                    new Field(ConfigConstant.DATA_TABLE_GROUP_ID, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATA_TARGET_TABLE_NAME, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATA_EVENT, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.DATA_ERROR, "LONGVARCHAR", Types.LONGVARCHAR),
                    new Field(ConfigConstant.BINLOG_DATA, "VARBINARY", Types.BLOB),
                    new Field(ConfigConstant.TASK_STATUS, "INTEGER", Types.INTEGER),
                    new Field(ConfigConstant.TASK_ID, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.SOURCE_TABLE_NAME, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.TARGET_TABLE_NAME, "VARCHAR", Types.VARCHAR),
                    new Field(ConfigConstant.CONTENT, "VARCHAR", Types.VARCHAR)
            ).peek(field -> {
                field.setLabelName(field.getName());
                // 转换列下划线
                String labelName = UnderlineToCamelUtils.camelToUnderline(field.getName());
                field.setName(labelName);
            }).collect(Collectors.toMap(Field::getLabelName, field -> field));
        }

        public List<Field> getFields() {
            return fields;
        }

        public void build(String... fieldNames) {
            fields = new ArrayList<>(fieldNames.length);
            Stream.of(fieldNames).parallel().forEach(k -> {
                if (fieldMap.containsKey(k)) {
                    Field field = fieldMap.get(k);
                    fields.add(field);
                }
            });
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