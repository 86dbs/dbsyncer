package org.dbsyncer.storage.support;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.SetterEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.mysql.MysqlConnector;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.connector.util.JDBCUtil;
import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Param;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.storage.support.mysql", havingValue = "true")
@ConfigurationProperties(prefix = "dbsyncer.storage.support.mysql")
public class MysqlStorageServiceImpl extends AbstractStorageService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String PREFIX_TABLE = "dbsyncer_";
    private static final String SHOW_TABLE = "show tables where Tables_in_%s = \"%s\"";
    private static final String DROP_TABLE = "DROP TABLE %s";
    private static final String TRUNCATE_TABLE = "TRUNCATE TABLE %s";
    private static final String TABLE_CREATE_TIME = "create_time";
    private static final String TABLE_UPDATE_TIME = "update_time";

    @Autowired
    private ConnectorFactory connectorFactory;

    private Map<String, Executor> tables = new ConcurrentHashMap<>();

    private Database connector = new MysqlConnector();

    private JdbcTemplate jdbcTemplate;

    private DatabaseConfig config;

    private String database;

    @PostConstruct
    private void init() {
        logger.info("url:{}", config.getUrl());
        logger.info("driverClassName:{}", config.getDriverClassName());
        logger.info("username:{}", config.getUsername());
        logger.info("password:{}", config.getPassword());
        ConnectorConfig cfg = config;
        cfg.setConnectorType(ConnectorEnum.MYSQL.getType());
        jdbcTemplate = DatabaseUtil.getJdbcTemplate(config);

        // 获取数据库名称
        Connection conn = null;
        try {
            conn = jdbcTemplate.getDataSource().getConnection();
            database = DatabaseUtil.getDataBaseName(conn);
        } catch (Exception e) {
            logger.error("无法连接Mysql,URL:{}", config.getUrl());
            throw new StorageException(e.getMessage());
        } finally {
            JDBCUtil.close(conn);
        }

        // 初始化表
        initTable();
    }

    @Override
    public Paging select(Query query) {
        Executor executor = getExecutor(query.getType(), query.getCollection());
        List<Object> queryArgs = new ArrayList<>();
        List<Object> queryCountArgs = new ArrayList<>();
        String querySql = buildQuerySql(query, executor, queryArgs);
        String queryCountSql = buildQueryCountSql(query, executor, queryCountArgs);

        List<Map<String, Object>> data = jdbcTemplate.queryForList(querySql, queryArgs.toArray());
        replaceHighLight(query, data);
        Long total = jdbcTemplate.queryForObject(queryCountSql, queryCountArgs.toArray(), Long.class);

        Paging paging = new Paging(query.getPageNum(), query.getPageSize());
        paging.setData(data);
        paging.setTotal(total);
        return paging;
    }

    @Override
    public void insert(StorageEnum type, String table, Map params) {
        executeInsert(type, table, params);
    }

    @Override
    public void update(StorageEnum type, String table, Map params) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getUpdate();
        List<Object> args = getParams(executor, params);
        args.add(params.get(ConfigConstant.CONFIG_MODEL_ID));
        int update = jdbcTemplate.update(sql, args.toArray());
        Assert.isTrue(update > 0, "update failed");
    }

    @Override
    public void delete(StorageEnum type, String table, String id) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getDelete();
        int delete = jdbcTemplate.update(sql, new Object[]{id});
        Assert.isTrue(delete > 0, "delete failed");
    }

    @Override
    public void deleteAll(StorageEnum type, String table) {
        Executor executor = getExecutor(type, table);
        if (executor.isSystemType()) {
            String sql = String.format(TRUNCATE_TABLE, PREFIX_TABLE.concat(table));
            jdbcTemplate.execute(sql);
            return;
        }

        if (tables.containsKey(table)) {
            tables.remove(table);
            String sql = String.format(DROP_TABLE, PREFIX_TABLE.concat(table));
            jdbcTemplate.execute(sql);
        }
    }

    @Override
    public void insertLog(StorageEnum type, String table, Map<String, Object> params) {
        executeInsert(type, table, params);
    }

    @Override
    public void insertData(StorageEnum type, String table, List<Map> list) {
        if (!CollectionUtils.isEmpty(list)) {
            Executor executor = getExecutor(type, table);
            Map<String, String> command = new HashMap<>();
            command.put(SqlBuilderEnum.INSERT.getName(), executor.getInsert());
            connectorFactory.writer(config, command, executor.getFields(), list);
        }

    }

    @Override
    public void destroy() throws Exception {
        DatabaseUtil.close(jdbcTemplate);
    }

    @Override
    protected String getSeparator() {
        return "_";
    }

    private int executeInsert(StorageEnum type, String table, Map params) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getInsert();
        List<Object> args = getParams(executor, params);
        int insert = jdbcTemplate.update(sql, args.toArray());
        if (insert < 1) {
            logger.error("table:{}, params:{}");
            throw new StorageException("insert failed");
        }
        return insert;
    }

    private List<Object> getParams(Executor executor, Map params) {
        return executor.getFieldPairs().stream().map(p -> params.get(p.labelName)).collect(Collectors.toList());
    }

    private Executor getExecutor(StorageEnum type, String table) {
        // 获取模板
        Executor executor = tables.get(type.getType());
        Assert.notNull(executor, "未知的存储类型");

        synchronized (tables) {
            // 检查本地缓存
            Executor e = tables.get(table);
            if (tables.containsKey(table)) {
                return e;
            }
            // 不存在
            Executor newExecutor = new Executor(executor.getGroup(), executor.getFieldPairs(), executor.getFields(), executor.isDynamicTableName(), executor.isSystemType(), executor.isOrderByUpdateTime());
            createTableIfNotExist(table, newExecutor);

            tables.putIfAbsent(table, newExecutor);
            return newExecutor;
        }
    }

    private String buildQuerySql(Query query, Executor executor, List<Object> args) {
        StringBuilder sql = new StringBuilder(executor.getQuery());
        buildQuerySqlWithParams(query, args, sql);
        // order by updateTime,createTime desc
        sql.append(" order by ");
        if (executor.isOrderByUpdateTime()) {
            sql.append(ConfigConstant.CONFIG_MODEL_UPDATE_TIME).append(",");
        }
        sql.append(ConfigConstant.CONFIG_MODEL_CREATE_TIME).append(" desc");
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        args.add((query.getPageNum() - 1) * query.getPageSize());
        args.add(query.getPageSize());
        return sql.toString();
    }

    private String buildQueryCountSql(Query query, Executor executor, List<Object> args) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(ID) FROM (").append(executor.getQuery());
        buildQuerySqlWithParams(query, args, sql);
        sql.append(") _T");
        return sql.toString();
    }

    private void buildQuerySqlWithParams(Query query, List<Object> args, StringBuilder sql) {
        List<Param> params = query.getParams();
        if (!CollectionUtils.isEmpty(params)) {
            sql.append(" WHERE ");
            AtomicBoolean flag = new AtomicBoolean();
            params.forEach(p -> {
                if (flag.get()) {
                    sql.append(" AND ");
                }
                // name=?
                sql.append(p.getKey()).append(p.isHighlighter() ? " like ?" : "=?");
                args.add(p.isHighlighter() ? new StringBuilder("%").append(p.getValue()).append("%") : p.getValue());
                flag.compareAndSet(false, true);
            });
        }
    }

    private void initTable() {
        // 配置
        List<FieldPair> configFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.CONFIG_MODEL_NAME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_TYPE),
                new FieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, TABLE_UPDATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        List<Field> cfields = Arrays.asList(
                new Field(ConfigConstant.CONFIG_MODEL_ID, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), true),
                new Field(ConfigConstant.CONFIG_MODEL_NAME, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_TYPE, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_JSON, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType(), false));

        // 日志
        List<FieldPair> logFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.CONFIG_MODEL_TYPE),
                new FieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        List<Field> lfields = Arrays.asList(
                new Field(ConfigConstant.CONFIG_MODEL_ID, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), true),
                new Field(ConfigConstant.CONFIG_MODEL_TYPE, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_JSON, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType(), false));

        // 数据
        List<FieldPair> dataFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.DATA_SUCCESS),
                new FieldPair(ConfigConstant.DATA_EVENT),
                new FieldPair(ConfigConstant.DATA_ERROR),
                new FieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        List<Field> dfields = Arrays.asList(
                new Field(ConfigConstant.CONFIG_MODEL_ID, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), true),
                new Field(ConfigConstant.DATA_SUCCESS, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), false),
                new Field(ConfigConstant.DATA_EVENT, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), false),
                new Field(ConfigConstant.DATA_ERROR, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType(), false),
                new Field(ConfigConstant.CONFIG_MODEL_JSON, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType(), false));

        tables.putIfAbsent(StorageEnum.CONFIG.getType(), new Executor(StorageEnum.CONFIG, configFields, cfields, false, true, true));
        tables.putIfAbsent(StorageEnum.LOG.getType(), new Executor(StorageEnum.LOG, logFields, lfields, false, true, false));
        tables.putIfAbsent(StorageEnum.DATA.getType(), new Executor(StorageEnum.DATA, dataFields, dfields, true, false, false));
        // 创建表
        tables.forEach((tableName, e) -> {
            if (!e.isDynamicTableName()) {
                createTableIfNotExist(tableName, e);
            }
        });
    }

    private void createTableIfNotExist(String table, Executor executor) {
        table = PREFIX_TABLE.concat(table);
        // show tables where Tables_in_dbsyncer = "dbsyncer_config"
        String sql = String.format(SHOW_TABLE, database, table);
        try {
            jdbcTemplate.queryForMap(sql);
        } catch (EmptyResultDataAccessException e) {
            // 不存在表
            String type = executor.getGroup().getType();
            String template = PREFIX_TABLE.concat(type);
            String ddl = readSql("/".concat(template).concat(".sql"));
            // 动态替换表名
            ddl = executor.isDynamicTableName() ? StringUtils.replaceOnce(ddl, template, table) : ddl;
            logger.info(ddl);
            jdbcTemplate.execute(ddl);
        }

        List<String> fieldNames = new ArrayList<>();
        List<String> labelNames = new ArrayList<>();
        executor.getFieldPairs().forEach(p -> {
            fieldNames.add(p.columnName);
            labelNames.add(p.labelName);
        });
        final SqlBuilderConfig config = new SqlBuilderConfig(connector, table, ConfigConstant.CONFIG_MODEL_ID, fieldNames, labelNames, "",
                "");

        String query = SqlBuilderEnum.QUERY.getSqlBuilder().buildQuerySql(config);
        String insert = SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
        String update = SqlBuilderEnum.UPDATE.getSqlBuilder().buildSql(config);
        String delete = SqlBuilderEnum.DELETE.getSqlBuilder().buildSql(config);
        executor.setQuery(query).setInsert(insert).setUpdate(update).setDelete(delete);
    }

    private String readSql(String filePath) {
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
            close(bf);
            close(isr);
            close(in);
        }
        return res.toString();
    }

    private void close(Closeable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void replaceHighLight(Query query, List<Map<String, Object>> list) {
        // 开启高亮
        if (!CollectionUtils.isEmpty(list) && query.isEnableHighLightSearch()) {
            List<Param> highLight = query.getParams().stream().filter(p -> p.isHighlighter()).collect(Collectors.toList());
            list.parallelStream().forEach(row -> {
                highLight.forEach(p -> {
                    String text = String.valueOf(row.get(p.getKey()));
                    String replacement = new StringBuilder("<span style='color:red'>").append(p.getValue()).append("</span>").toString();
                    row.put(p.getKey(), StringUtils.replace(text, p.getValue(), replacement));
                });
            });
        }
    }

    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }

    class FieldPair {
        String labelName;
        String columnName;

        public FieldPair(String labelName) {
            this.labelName = labelName;
            this.columnName = labelName;
        }

        public FieldPair(String labelName, String columnName) {
            this.labelName = labelName;
            this.columnName = columnName;
        }
    }

    class Executor {
        private String query;
        private String insert;
        private String update;
        private String delete;
        private StorageEnum group;
        private List<FieldPair> fieldPairs;
        private List<Field> fields;
        private boolean dynamicTableName;
        private boolean systemType;
        private boolean orderByUpdateTime;

        public Executor(StorageEnum group, List<FieldPair> fieldPairs, List<Field> fields, boolean dynamicTableName, boolean systemType, boolean orderByUpdateTime) {
            this.group = group;
            this.fieldPairs = fieldPairs;
            this.fields = fields;
            this.dynamicTableName = dynamicTableName;
            this.systemType = systemType;
            this.orderByUpdateTime = orderByUpdateTime;
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

        public StorageEnum getGroup() {
            return group;
        }

        public List<Field> getFields() {
            return fields;
        }

        public List<FieldPair> getFieldPairs() {
            return fieldPairs;
        }

        public boolean isDynamicTableName() {
            return dynamicTableName;
        }

        public boolean isSystemType() {
            return systemType;
        }

        public boolean isOrderByUpdateTime() {
            return orderByUpdateTime;
        }

    }
}