package org.dbsyncer.storage.support;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.constant.DatabaseConstant;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.SetterEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
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
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 将数据存储在mysql
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.storage.support.mysql.enabled", havingValue = "true")
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

    private Database connector;

    private DatabaseConnectorMapper connectorMapper;

    private DatabaseConfig config;

    private String database;

    @PostConstruct
    private void init() throws InterruptedException {
        logger.info("url:{}", config.getUrl());
        config.setConnectorType(ConnectorEnum.MYSQL.getType());
        connectorMapper = (DatabaseConnectorMapper) connectorFactory.connect(config);
        connector = (Database) connectorFactory.getConnector(connectorMapper);

        // 获取数据库名称
        database = connectorMapper.execute(databaseTemplate -> {
            Connection conn = databaseTemplate.getConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            String driverVersion = metaData.getDriverVersion();
            String databaseProductVersion = metaData.getDatabaseProductVersion();
            boolean driverThanMysql8 = StringUtil.startsWith(driverVersion, "mysql-connector-java-8");
            boolean dbThanMysql8 = StringUtil.startsWith(databaseProductVersion, "8");
            Assert.isTrue(driverThanMysql8 == dbThanMysql8, String.format("当前驱动%s和数据库%s版本不一致.", driverVersion, databaseProductVersion));

            if(conn instanceof SimpleConnection){
                SimpleConnection simpleConnection = (SimpleConnection) conn;
                conn = simpleConnection.getConnection();
            }
            Class clazz = dbThanMysql8 ? conn.getClass() : conn.getClass().getSuperclass();
            java.lang.reflect.Field field = clazz.getDeclaredField("database");
            field.setAccessible(true);
            Object value = field.get(conn);
            return String.valueOf(value);
        });

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

        List<Map<String, Object>> data = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, queryArgs.toArray()));
        replaceHighLight(query, data);
        Long total = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(queryCountSql, queryCountArgs.toArray(), Long.class));

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
        int update = connectorMapper.execute(databaseTemplate -> databaseTemplate.update(sql, args.toArray()));
        Assert.isTrue(update > 0, "update failed");
    }

    @Override
    public void delete(StorageEnum type, String table, String id) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getDelete();
        int delete = connectorMapper.execute(databaseTemplate -> databaseTemplate.update(sql, new Object[]{id}));
        Assert.isTrue(delete > 0, "delete failed");
    }

    @Override
    public void deleteAll(StorageEnum type, String table) {
        Executor executor = getExecutor(type, table);
        if (executor.isSystemType()) {
            String sql = String.format(TRUNCATE_TABLE, PREFIX_TABLE.concat(table));
            executeSql(sql);
            return;
        }

        if (tables.containsKey(table)) {
            tables.remove(table);
            String sql = String.format(DROP_TABLE, PREFIX_TABLE.concat(table));
            executeSql(sql);
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
            ConnectorMapper connectorMapper = connectorFactory.connect(config);
            connectorFactory.writer(connectorMapper, new WriterBatchConfig(table, ConnectorConstant.OPERTION_INSERT, command, executor.getFields(), list));
        }

    }

    @Override
    public void destroy() {
        connectorMapper.close();
    }

    @Override
    protected String getSeparator() {
        return "_";
    }

    private int executeInsert(StorageEnum type, String table, Map params) {
        Executor executor = getExecutor(type, table);
        String sql = executor.getInsert();
        List<Object> args = getParams(executor, params);
        int insert = connectorMapper.execute(databaseTemplate -> databaseTemplate.update(sql, args.toArray()));
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

        if (tables.containsKey(table)) {
            return tables.get(table);
        }
        synchronized (tables) {
            // 检查本地缓存
            if (tables.containsKey(table)) {
                return tables.get(table);
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
        StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ").append(executor.getTable());
        buildQuerySqlWithParams(query, args, sql);
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
                sql.append(p.getKey()).append(p.isHighlighter() ? " LIKE ?" : "=?");
                args.add(p.isHighlighter() ? new StringBuilder("%").append(p.getValue()).append("%") : p.getValue());
                flag.compareAndSet(false, true);
            });
        }
    }

    private void initTable() throws InterruptedException {
        // 配置
        FieldBuilder builder = new FieldBuilder();
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_NAME, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<FieldPair> configFields = builder.getFieldPairs();
        List<Field> cfields = builder.getFields();

        // 日志
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<FieldPair> logFields = builder.getFieldPairs();
        List<Field> lfields = builder.getFields();

        // 数据
        builder.build(ConfigConstant.CONFIG_MODEL_ID, ConfigConstant.DATA_SUCCESS, ConfigConstant.DATA_EVENT, ConfigConstant.DATA_ERROR, ConfigConstant.CONFIG_MODEL_CREATE_TIME, ConfigConstant.CONFIG_MODEL_JSON);
        List<FieldPair> dataFields = builder.getFieldPairs();
        List<Field> dfields = builder.getFields();

        tables.putIfAbsent(StorageEnum.CONFIG.getType(), new Executor(StorageEnum.CONFIG, configFields, cfields, false, true, true));
        tables.putIfAbsent(StorageEnum.LOG.getType(), new Executor(StorageEnum.LOG, logFields, lfields, false, true, false));
        tables.putIfAbsent(StorageEnum.DATA.getType(), new Executor(StorageEnum.DATA, dataFields, dfields, true, false, false));
        // 创建表
        tables.forEach((tableName, e) -> {
            if (!e.isDynamicTableName()) {
                createTableIfNotExist(tableName, e);
            }
        });

        // wait few seconds for execute sql
        TimeUnit.SECONDS.sleep(1);
    }

    private void createTableIfNotExist(String table, Executor executor) {
        table = PREFIX_TABLE.concat(table);
        // show tables where Tables_in_dbsyncer = "dbsyncer_config"
        String sql = String.format(SHOW_TABLE, database, table);
        try {
            connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForMap(sql));
        } catch (EmptyResultDataAccessException e) {
            // 不存在表
            String type = executor.getGroup().getType();
            String template = PREFIX_TABLE.concat(type);
            String ddl = readSql("/".concat(template).concat(".sql"));
            // 动态替换表名
            ddl = executor.isDynamicTableName() ? StringUtil.replaceOnce(ddl, template, table) : ddl;
            logger.info(ddl);
            executeSql(ddl);
        }

        List<Field> fields = executor.getFieldPairs().stream().map(p -> new Field(p.columnName, p.labelName)).collect(Collectors.toList());
        final SqlBuilderConfig config = new SqlBuilderConfig(connector, table, ConfigConstant.CONFIG_MODEL_ID, fields, "", "");

        String query = SqlBuilderEnum.QUERY.getSqlBuilder().buildQuerySql(config);
        String insert = SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
        String update = SqlBuilderEnum.UPDATE.getSqlBuilder().buildSql(config);
        String delete = SqlBuilderEnum.DELETE.getSqlBuilder().buildSql(config);
        executor.setTable(table).setQuery(query).setInsert(insert).setUpdate(update).setDelete(delete);
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

    private void executeSql(String ddl) {
        connectorMapper.execute(databaseTemplate -> {
            databaseTemplate.execute(ddl);
            return true;
        });
    }

    private void replaceHighLight(Query query, List<Map<String, Object>> list) {
        // 开启高亮
        if (!CollectionUtils.isEmpty(list) && query.isEnableHighLightSearch()) {
            List<Param> highLight = query.getParams().stream().filter(p -> p.isHighlighter()).collect(Collectors.toList());
            list.forEach(row ->
                highLight.forEach(p -> {
                    String text = String.valueOf(row.get(p.getKey()));
                    String replacement = new StringBuilder("<span style='color:red'>").append(p.getValue()).append("</span>").toString();
                    row.put(p.getKey(), StringUtil.replace(text, p.getValue(), replacement));
                })
            );
        }
    }

    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }

    final class FieldPair {
        String columnName;
        String labelName;

        public FieldPair(String columnName) {
            this.columnName = columnName;
            this.labelName = columnName;
        }

        public FieldPair(String columnName, String labelName) {
            this.columnName = columnName;
            this.labelName = labelName;
        }
    }

    final class FieldBuilder {
        Map<String, FieldPair> fieldPairMap = new ConcurrentHashMap<>();
        Map<String, Field> fieldMap = new ConcurrentHashMap<>();
        List<FieldPair> fieldPairs;
        List<Field> fields;

        public FieldBuilder() {
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_ID, new FieldPair(ConfigConstant.CONFIG_MODEL_ID));
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_NAME, new FieldPair(ConfigConstant.CONFIG_MODEL_NAME));
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_TYPE, new FieldPair(ConfigConstant.CONFIG_MODEL_TYPE));
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_CREATE_TIME, new FieldPair(TABLE_CREATE_TIME, ConfigConstant.CONFIG_MODEL_CREATE_TIME));
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, new FieldPair(TABLE_UPDATE_TIME, ConfigConstant.CONFIG_MODEL_UPDATE_TIME));
            fieldPairMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_JSON, new FieldPair(ConfigConstant.CONFIG_MODEL_JSON));
            fieldPairMap.putIfAbsent(ConfigConstant.DATA_SUCCESS, new FieldPair(ConfigConstant.DATA_SUCCESS));
            fieldPairMap.putIfAbsent(ConfigConstant.DATA_EVENT, new FieldPair(ConfigConstant.DATA_EVENT));
            fieldPairMap.putIfAbsent(ConfigConstant.DATA_ERROR, new FieldPair(ConfigConstant.DATA_ERROR));

            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_ID, new Field(ConfigConstant.CONFIG_MODEL_ID, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType(), true));
            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_NAME, new Field(ConfigConstant.CONFIG_MODEL_NAME, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType()));
            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_TYPE, new Field(ConfigConstant.CONFIG_MODEL_TYPE, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType()));
            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_CREATE_TIME, new Field(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType()));
            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, new Field(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SetterEnum.BIGINT.name(), SetterEnum.BIGINT.getType()));
            fieldMap.putIfAbsent(ConfigConstant.CONFIG_MODEL_JSON, new Field(ConfigConstant.CONFIG_MODEL_JSON, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType()));
            fieldMap.putIfAbsent(ConfigConstant.DATA_SUCCESS, new Field(ConfigConstant.DATA_SUCCESS, SetterEnum.INTEGER.name(), SetterEnum.INTEGER.getType()));
            fieldMap.putIfAbsent(ConfigConstant.DATA_EVENT, new Field(ConfigConstant.DATA_EVENT, SetterEnum.VARCHAR.name(), SetterEnum.VARCHAR.getType()));
            fieldMap.putIfAbsent(ConfigConstant.DATA_ERROR, new Field(ConfigConstant.DATA_ERROR, SetterEnum.LONGVARCHAR.name(), SetterEnum.LONGVARCHAR.getType()));
        }

        public List<FieldPair> getFieldPairs() {
            return fieldPairs;
        }

        public List<Field> getFields() {
            return fields;
        }

        public void build(String... fieldNames) {
            fieldPairs = new ArrayList<>(fieldNames.length);
            fields = new ArrayList<>(fieldNames.length);
            Stream.of(fieldNames).parallel().forEach(k -> {
                fieldPairs.add(fieldPairMap.get(k));
                fields.add(fieldMap.get(k));
            });
        }
    }

    final class Executor {
        private String table;
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