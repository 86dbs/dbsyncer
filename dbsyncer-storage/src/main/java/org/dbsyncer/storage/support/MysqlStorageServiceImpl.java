package org.dbsyncer.storage.support;

import org.apache.commons.dbcp.DelegatingDatabaseMetaData;
import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.mysql.MysqlConnector;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.connector.util.JDBCUtil;
import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final String PK = ConfigConstant.CONFIG_MODEL_ID;

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
        getExecutor(query.getType(), query.getCollection());

        return null;
    }

    @Override
    public void insert(StorageEnum type, String table, Map params) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void update(StorageEnum type, String table, Map params) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void delete(StorageEnum type, String table, String id) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void deleteAll(StorageEnum type, String table) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void insertLog(StorageEnum type, String table, Map<String, Object> params) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void insertData(StorageEnum type, String table, List<Map> list) {
        getExecutor(type.getType(), table);

    }

    @Override
    public void destroy() throws Exception {
        DatabaseUtil.close(jdbcTemplate);
    }

    @Override
    protected String getSeparator() {
        return "_";
    }

    private Executor getExecutor(String group, String table) {
        // 获取模板
        Executor executor = tables.get(group);
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
        List<String> configFields = Arrays.asList(
                ConfigConstant.CONFIG_MODEL_ID,
                ConfigConstant.CONFIG_MODEL_NAME,
                ConfigConstant.CONFIG_MODEL_TYPE,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME,
                ConfigConstant.CONFIG_MODEL_UPDATE_TIME,
                ConfigConstant.CONFIG_MODEL_JSON);
        // 日志
        List<String> logFields = Arrays.asList(
                ConfigConstant.CONFIG_MODEL_ID,
                ConfigConstant.CONFIG_MODEL_NAME,
                ConfigConstant.CONFIG_MODEL_TYPE,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME,
                ConfigConstant.CONFIG_MODEL_JSON);
        // 数据
        List<String> dataFields = Arrays.asList(
                ConfigConstant.CONFIG_MODEL_ID,
                ConfigConstant.CONFIG_MODEL_NAME,
                ConfigConstant.DATA_SUCCESS,
                ConfigConstant.DATA_EVENT,
                ConfigConstant.DATA_ERROR,
                ConfigConstant.CONFIG_MODEL_CREATE_TIME,
                ConfigConstant.CONFIG_MODEL_JSON);
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

        List<String> fieldName = executor.getFieldName();
        String query = SqlBuilderEnum.QUERY.getSqlBuilder().buildSql(table, PK, fieldName, "", "", connector);
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

    class Executor {
        private String       query;
        private String       insert;
        private String       update;
        private String       delete;
        private StorageEnum  group;
        private List<String> fieldName;
        private boolean      dynamicTableName;

        public Executor() {
        }

        public Executor(StorageEnum group, List<String> fieldName) {
            this.group = group;
            this.fieldName = fieldName;
        }

        public Executor(StorageEnum group, List<String> fieldName, boolean dynamicTableName) {
            this.group = group;
            this.fieldName = fieldName;
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

        public List<String> getFieldName() {
            return fieldName;
        }

        public boolean isDynamicTableName() {
            return dynamicTableName;
        }
    }
}