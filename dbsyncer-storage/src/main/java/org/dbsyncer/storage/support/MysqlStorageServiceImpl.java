package org.dbsyncer.storage.support;

import org.apache.commons.dbcp.DelegatingDatabaseMetaData;
import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.database.sqlbuilder.SqlBuilderQuery;
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
import org.springframework.beans.BeanUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    private static final String PREFIX_TABLE      = "dbsyncer_";
    private static final String SHOW_TABLE        = "show tables where Tables_in_%s = \"%s\"";
    private static final String TRUNCATE_TABLE    = "TRUNCATE TABLE \"%s\"";
    private static final String PK                = ConfigConstant.CONFIG_MODEL_ID;
    private static final String TABLE_CREATE_TIME = "create_time";
    private static final String TABLE_UPDATE_TIME = "update_time";

    private Map<String, Executor> tables = new ConcurrentHashMap<>();

    private Database connector = new MysqlConnector();

    private JdbcTemplate jdbcTemplate;

    private DatabaseConfig config;

    private String database;

    @PostConstruct
    private void init() {
        config = null == config ? new DatabaseConfig() : config;
        config.setUrl(StringUtils.isNotBlank(config.getUrl()) ? config.getUrl()
                : "jdbc:mysql://127.0.0.1:3306/dbsyncer?rewriteBatchedStatements=true&seUnicode=true&characterEncoding=UTF8&useSSL=true");
        config.setDriverClassName(
                StringUtils.isNotBlank(config.getDriverClassName()) ? config.getDriverClassName() : "com.mysql.jdbc.Driver");
        config.setUsername(StringUtils.isNotBlank(config.getUsername()) ? config.getUsername() : "root");
        config.setPassword(StringUtils.isNotBlank(config.getPassword()) ? config.getPassword() : "123");
        logger.info("url:{}", config.getUrl());
        logger.info("driverClassName:{}", config.getDriverClassName());
        logger.info("username:{}", config.getUsername());
        logger.info("password:{}", config.getPassword());
        jdbcTemplate = DatabaseUtil.getJdbcTemplate(config);

        // 获取数据库名称
        Connection conn = null;
        try {
            conn = jdbcTemplate.getDataSource().getConnection();
            DelegatingDatabaseMetaData md = (DelegatingDatabaseMetaData) conn.getMetaData();
            DatabaseMetaData delegate = md.getDelegate();
            Class clazz = delegate.getClass().getSuperclass();
            Field field = clazz.getDeclaredField("database");
            field.setAccessible(true);
            database = (String) field.get(delegate);
        } catch (Exception e) {
            throw new StorageException(e.getMessage());
        } finally {
            JDBCUtil.close(conn);
        }

        // 初始化表
        initTable();
    }

    @Override
    public List<Map> select(Query query) {
        Executor executor = getExecutor(query.getType(), query.getCollection());
        List<Object> args = new ArrayList<>();
        String sql = buildQuerySql(query, executor, args);

        List<Map> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, args.toArray());
        result.addAll(list);
        return result;
    }

    @Override
    public void insert(StorageEnum type, String table, Map params) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getInsert();
        List<Object> args = getParams(executor, params);
        int insert = jdbcTemplate.update(sql, args.toArray());
        Assert.isTrue(insert > 0, "insert failed");
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
        int delete = jdbcTemplate.update(sql, new Object[] {id});
        Assert.isTrue(delete > 0, "delete failed");
    }

    @Override
    public void deleteAll(StorageEnum type, String table) {
        String sql = String.format(TRUNCATE_TABLE, table);
        jdbcTemplate.execute(sql);
    }

    @Override
    public void insertLog(StorageEnum type, String table, Map<String, Object> params) {
        getExecutor(type, table);

    }

    @Override
    public void insertData(StorageEnum type, String table, List<Map> list) {
        getExecutor(type, table);

    }

    @Override
    public void destroy() throws Exception {
        DatabaseUtil.close(jdbcTemplate);
    }

    @Override
    protected String getSeparator() {
        return "_";
    }

    private List<Object> getParams(Executor executor, Map params) {
        return executor.getFieldPairs().stream().map(p -> p.convert(params.get(p.labelName))).collect(Collectors.toList());
    }

    private String buildQuerySql(Query query, Executor executor, List<Object> args) {
        StringBuilder sql = new StringBuilder(executor.getQuery());
        List<Param> params = query.getParams();
        if (!CollectionUtils.isEmpty(params)) {
            sql.append(" WHERE ");
            AtomicBoolean flag = new AtomicBoolean();
            params.forEach(p -> {
                if (flag.get()) {
                    sql.append(" AND ");
                }
                // name=?
                sql.append(p.getKey()).append("=").append("?");
                args.add(p.getValue());
                flag.compareAndSet(false, true);
            });
        }
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        args.add((query.getPageNum() - 1) * query.getPageSize());
        args.add(query.getPageSize());
        return sql.toString();
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
            Executor newExecutor = new Executor();
            BeanUtils.copyProperties(executor, newExecutor);
            createTableIfNotExist(table, newExecutor);

            tables.putIfAbsent(table, newExecutor);
            return newExecutor;
        }
    }

    private void initTable() {
        // 配置
        List<FieldPair> configFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.CONFIG_MODEL_NAME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_TYPE),
                new TimestampFieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new TimestampFieldPair(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, TABLE_UPDATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        // 日志
        List<FieldPair> logFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.CONFIG_MODEL_NAME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_TYPE),
                new TimestampFieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        // 数据
        List<FieldPair> dataFields = Arrays.asList(
                new FieldPair(ConfigConstant.CONFIG_MODEL_ID),
                new FieldPair(ConfigConstant.CONFIG_MODEL_NAME),
                new FieldPair(ConfigConstant.DATA_SUCCESS),
                new FieldPair(ConfigConstant.DATA_EVENT),
                new FieldPair(ConfigConstant.DATA_ERROR),
                new TimestampFieldPair(ConfigConstant.CONFIG_MODEL_CREATE_TIME, TABLE_CREATE_TIME),
                new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
        tables.putIfAbsent(StorageEnum.CONFIG.getType(), new Executor(StorageEnum.CONFIG, configFields));
        tables.putIfAbsent(StorageEnum.LOG.getType(), new Executor(StorageEnum.LOG, logFields));
        tables.putIfAbsent(StorageEnum.DATA.getType(), new Executor(StorageEnum.DATA, dataFields, true));
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

        List<String> fieldName = executor.getFieldPairs().stream().map(p -> p.columnName).collect(Collectors.toList());
        String query = new SqlBuilderQuery().buildStandardSql(table, PK, fieldName, "", "");
        String insert = SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(table, PK, fieldName, "", "", connector);
        String update = SqlBuilderEnum.UPDATE.getSqlBuilder().buildSql(table, PK, fieldName, "", "", connector);
        String delete = SqlBuilderEnum.DELETE.getSqlBuilder().buildSql(table, PK, fieldName, "", "", connector);
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

    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }

    interface FieldHandler {
        Object convert(Object val);
    }

    class FieldPair {
        String       labelName;
        String       columnName;
        FieldHandler handler;

        public FieldPair(String labelName) {
            this.labelName = labelName;
            this.columnName = labelName;
            this.handler = val -> val;
        }

        public FieldPair(String labelName, String columnName) {
            this.labelName = labelName;
            this.columnName = columnName;
        }

        public Object convert(Object val) {
            return val;
        }
    }

    class TimestampFieldPair extends FieldPair {

        public TimestampFieldPair(String labelName, String columnName) {
            super(labelName, columnName);
        }

        @Override
        public Object convert(Object val) {
            return new Timestamp((Long) val);
        }
    }

    class Executor {
        private String          query;
        private String          insert;
        private String          update;
        private String          delete;
        private StorageEnum     group;
        private List<FieldPair> fieldPairs;
        private boolean         dynamicTableName;

        public Executor() {
        }

        public Executor(StorageEnum group, List<FieldPair> fieldPairs) {
            this.group = group;
            this.fieldPairs = fieldPairs;
        }

        public Executor(StorageEnum group, List<FieldPair> fieldPairs, boolean dynamicTableName) {
            this.group = group;
            this.fieldPairs = fieldPairs;
            this.dynamicTableName = dynamicTableName;
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

        public List<FieldPair> getFieldPairs() {
            return fieldPairs;
        }

        public boolean isDynamicTableName() {
            return dynamicTableName;
        }
    }
}