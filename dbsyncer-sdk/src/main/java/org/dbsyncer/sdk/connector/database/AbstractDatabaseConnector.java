package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.config.WriterBatchConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractDatabaseConnector extends AbstractConnector implements ConnectorService<DatabaseConnectorInstance, DatabaseConfig>, Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 系统函数表达式$convert()$
     */
    private final String SYS_EXPRESSION = "^[$].*[$]$";

    @Override
    public boolean isSupportedTiming() {
        return true;
    }

    @Override
    public boolean isSupportedLog() {
        return true;
    }

    @Override
    public Class<DatabaseConfig> getConfigClass() {
        return DatabaseConfig.class;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config) {
        return new DatabaseConnectorInstance(config);
    }

    @Override
    public void disconnect(DatabaseConnectorInstance connectorMapper) {
        connectorMapper.close();
    }

    @Override
    public boolean isAlive(DatabaseConnectorInstance connectorMapper) {
        Integer count = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(getValidationQuery(), Integer.class));
        return null != count && count > 0;
    }

    @Override
    public String getConnectorInstanceCacheKey(DatabaseConfig config) {
        return String.format("%s-%s-%s", config.getConnectorType(), config.getUrl(), config.getUsername());
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorMapper) {
        return getTable(connectorMapper, null, getSchema(connectorMapper.getConfig()), null);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorInstance connectorMapper, String tableNamePattern) {
        List<Field> fields = new ArrayList<>();
        final String schema = getSchema(connectorMapper.getConfig());
        connectorMapper.execute(databaseTemplate -> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            String catalog = conn.getCatalog();
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            DatabaseMetaData metaData = conn.getMetaData();
            List<String> primaryKeys = findTablePrimaryKeys(metaData, catalog, schemaNamePattern, tableNamePattern);
            ResultSet columnMetadata = metaData.getColumns(catalog, schemaNamePattern, tableNamePattern, null);
            while (columnMetadata.next()) {
                String columnName = columnMetadata.getString(4);
                int columnType = columnMetadata.getInt(5);
                String typeName = columnMetadata.getString(6);
                fields.add(new Field(columnName, typeName, columnType, primaryKeys.contains(columnName)));
            }
            return fields;
        });
        return new MetaInfo().setColumn(fields);
    }

    @Override
    public long getCount(DatabaseConnectorInstance connectorMapper, Map<String, String> command) {
        if (CollectionUtils.isEmpty(command)) {
            return 0L;
        }

        // 1、获取select SQL
        String queryCountSql = command.get(ConnectorConstant.OPERTION_QUERY_COUNT);
        if (StringUtil.isBlank(queryCountSql)) {
            return 0;
        }

        // 2、返回结果集
        return connectorMapper.execute(databaseTemplate -> {
            Long count = databaseTemplate.queryForObject(queryCountSql, Long.class);
            return count == null ? 0 : count;
        });
    }

    @Override
    public Result reader(DatabaseConnectorInstance connectorMapper, ReaderConfig config) {
        // 1、获取select SQL
        boolean supportedCursor = enableCursor() && config.isSupportedCursor() && null != config.getCursors();
        String queryKey = supportedCursor ? ConnectorConstant.OPERTION_QUERY_CURSOR : ConnectorConstant.OPERTION_QUERY;
        String querySql = config.getCommand().get(queryKey);
        Assert.hasText(querySql, "查询语句不能为空.");

        // 2、设置参数
        Collections.addAll(config.getArgs(), supportedCursor ? getPageCursorArgs(config) : getPageArgs(config));

        // 3、执行SQL
        List<Map<String, Object>> list = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, config.getArgs().toArray()));

        // 4、返回结果集
        return new Result(list);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorMapper, WriterBatchConfig config) {
        String event = config.getEvent();
        List<Map> data = config.getData();

        // 1、获取SQL
        String executeSql = config.getCommand().get(event);
        Assert.hasText(executeSql, "执行SQL语句不能为空.");
        if (CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer fields can not be empty.");
            throw new SdkException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new SdkException("writer data can not be empty.");
        }
        List<Field> fields = new ArrayList<>(config.getFields());
        List<Field> pkFields = PrimaryKeyUtil.findConfigPrimaryKeyFields(config);
        // Update / Delete
        if (!isInsert(event)) {
            if (isDelete(event)) {
                fields.clear();
            } else if (isUpdate(event)) {
                removeFieldWithPk(fields, pkFields);
            }
            fields.addAll(pkFields);
        }

        Result result = new Result();
        int[] execute = null;
        try {
            // 2、设置参数
            execute = connectorMapper.execute(databaseTemplate -> databaseTemplate.batchUpdate(executeSql, batchRows(fields, data)));
        } catch (Exception e) {
            data.forEach(row -> forceUpdate(result, connectorMapper, config, pkFields, row));
        }

        if (null != execute) {
            int batchSize = execute.length;
            for (int i = 0; i < batchSize; i++) {
                if (execute[i] == 1 || execute[i] == -2) {
                    result.getSuccessData().add(data.get(i));
                    continue;
                }
                forceUpdate(result, connectorMapper, config, pkFields, data.get(i));
            }
        }
        return result;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return new HashMap<>();
        }

        String tableName = table.getName();
        if (StringUtil.isBlank(tableName)) {
            logger.error("数据源表不能为空.");
            throw new SdkException("数据源表不能为空.");
        }

        // 架构名
        String schema = getSchemaWithQuotation((DatabaseConfig) commandConfig.getConnectorConfig());
        // 同步字段
        List<Field> column = filterColumn(table.getColumn());
        // 获取过滤SQL
        final String queryFilterSql = getQueryFilterSql(commandConfig);
        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, column, queryFilterSql);

        // 获取查询SQL
        Map<String, String> map = new HashMap<>();
        buildSql(map, SqlBuilderEnum.QUERY, config);
        // 是否支持游标
        if (enableCursor()) {
            buildSql(map, SqlBuilderEnum.QUERY_CURSOR, config);
        }
        // 获取查询总数SQL
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, getQueryCountSql(commandConfig, primaryKeys, schema, queryFilterSql));
        return map;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return new HashMap<>();
        }

        String tableName = table.getName();
        if (StringUtil.isBlank(tableName)) {
            logger.error("目标源表不能为空.");
            throw new SdkException("目标源表不能为空.");
        }

        // 架构名
        String schema = getSchemaWithQuotation((DatabaseConfig) commandConfig.getConnectorConfig());
        // 同步字段
        List<Field> column = filterColumn(table.getColumn());
        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, column, null);

        // 获取增删改SQL
        Map<String, String> map = new HashMap<>();
        buildSql(map, SqlBuilderEnum.INSERT, config);
        buildSql(map, SqlBuilderEnum.UPDATE, config);
        buildSql(map, SqlBuilderEnum.DELETE, config);
        buildSql(map, SqlBuilderEnum.QUERY_EXIST, config);
        return map;
    }

    /**
     * 获取架构名
     *
     * @param config
     * @return
     */
    protected String getSchema(DatabaseConfig config) {
        return config.getSchema();
    }

    /**
     * 获取架构名
     *
     * @param config
     * @return
     */
    protected String getSchemaWithQuotation(DatabaseConfig config) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            String quotation = buildSqlWithQuotation();
            schema.append(quotation).append(config.getSchema()).append(quotation).append(".").toString();
        }
        return schema.toString();
    }

    /**
     * 满足游标查询条件，追加主键排序
     *
     * @param config
     * @param sql
     */
    protected void appendOrderByPk(PageSql config, StringBuilder sql) {
        sql.append(" ORDER BY ");
        final String quotation = buildSqlWithQuotation();
        PrimaryKeyUtil.buildSql(sql, config.getPrimaryKeys(), quotation, ",", "", true);
    }

    /**
     * 获取数据库表元数据信息
     *
     * @param databaseTemplate
     * @param metaSql          查询元数据
     * @param schema           架构名
     * @param tableName        表名
     * @return
     */
    protected MetaInfo getMetaInfo(DatabaseTemplate databaseTemplate, String metaSql, String schema, String tableName) throws SQLException {
        SqlRowSet sqlRowSet = databaseTemplate.queryForRowSet(metaSql);
        ResultSetWrappingSqlRowSet rowSet = (ResultSetWrappingSqlRowSet) sqlRowSet;
        SqlRowSetMetaData metaData = rowSet.getMetaData();

        // 查询表字段信息
        int columnCount = metaData.getColumnCount();
        if (1 > columnCount) {
            throw new SdkException("查询表字段不能为空.");
        }
        List<Field> fields = new ArrayList<>(columnCount);
        Map<String, List<String>> tables = new HashMap<>();
        try {
            Connection connection = databaseTemplate.getSimpleConnection();
            DatabaseMetaData md = connection.getMetaData();
            final String catalog = connection.getCatalog();
            schema = StringUtil.isNotBlank(schema) ? schema : null;
            String name = null;
            String label = null;
            String typeName = null;
            String table = null;
            int columnType;
            boolean pk;
            for (int i = 1; i <= columnCount; i++) {
                table = StringUtil.isNotBlank(tableName) ? tableName : metaData.getTableName(i);
                if (null == tables.get(table)) {
                    tables.putIfAbsent(table, findTablePrimaryKeys(md, catalog, schema, table));
                }
                name = metaData.getColumnName(i);
                label = metaData.getColumnLabel(i);
                typeName = metaData.getColumnTypeName(i);
                columnType = metaData.getColumnType(i);
                pk = isPk(tables, table, name);
                fields.add(new Field(label, typeName, columnType, pk));
            }
        } finally {
            tables.clear();
        }
        return new MetaInfo().setColumn(fields);
    }

    /**
     * 获取查询总数SQL
     *
     * @param commandConfig
     * @param primaryKeys
     * @param schema
     * @param queryFilterSql
     * @return
     */
    protected String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        Table table = commandConfig.getTable();
        String tableName = table.getName();
        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, table.getColumn(), queryFilterSql);
        return SqlBuilderEnum.QUERY_COUNT.getSqlBuilder().buildSql(config);
    }

    /**
     * 获取查询条件SQL
     *
     * @param commandConfig
     * @return
     */
    protected String getQueryFilterSql(CommandConfig commandConfig) {
        List<Filter> filter = commandConfig.getFilter();
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        Table table = commandConfig.getTable();
        Map<String, Field> fieldMap = new HashMap<>();
        table.getColumn().forEach(field -> fieldMap.put(field.getName(), field));

        // 过滤条件SQL
        StringBuilder sql = new StringBuilder();

        // 拼接并且SQL
        String addSql = buildFilterSql(fieldMap, OperationEnum.AND.getName(), filter);
        // 如果Add条件存在
        if (StringUtil.isNotBlank(addSql)) {
            sql.append(addSql);
        }

        // 拼接或者SQL
        String orSql = buildFilterSql(fieldMap, OperationEnum.OR.getName(), filter);
        // 如果Or条件和Add条件都存在
        if (StringUtil.isNotBlank(orSql) && StringUtil.isNotBlank(addSql)) {
            sql.append(" OR ");
        }
        sql.append(orSql);

        // 如果有条件加上 WHERE
        if (StringUtil.isNotBlank(sql.toString())) {
            // WHERE (USER.USERNAME = 'zhangsan' AND USER.AGE='20') OR (USER.TEL='18299996666')
            sql.insert(0, " WHERE ");
        }
        return sql.toString();
    }

    /**
     * 去重列名
     *
     * @param column
     * @return
     */
    private List<Field> filterColumn(List<Field> column) {
        Set<String> mark = new HashSet<>();
        List<Field> fields = new ArrayList<>();
        for (Field c : column) {
            String name = c.getName();
            if (StringUtil.isBlank(name)) {
                throw new SdkException("The field name can not be empty.");
            }
            if (!mark.contains(name)) {
                fields.add(c);
                mark.add(name);
            }
        }
        return fields;
    }

    /**
     * 获取表列表
     *
     * @param connectorMapper
     * @param catalog
     * @param schema
     * @param tableNamePattern
     * @return
     */
    private List<Table> getTable(DatabaseConnectorInstance connectorMapper, String catalog, String schema, String tableNamePattern) {
        return connectorMapper.execute(databaseTemplate -> {
            List<Table> tables = new ArrayList<>();
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            String databaseCatalog = null == catalog ? conn.getCatalog() : catalog;
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            String[] types = {TableTypeEnum.TABLE.getCode(), TableTypeEnum.VIEW.getCode(), TableTypeEnum.MATERIALIZED_VIEW.getCode()};
            final ResultSet rs = conn.getMetaData().getTables(databaseCatalog, schemaNamePattern, tableNamePattern, types);
            while (rs.next()) {
                final String tableName = rs.getString("TABLE_NAME");
                final String tableType = rs.getString("TABLE_TYPE");
                tables.add(new Table(tableName, tableType));
            }
            return tables;
        });
    }

    /**
     * 根据过滤条件获取查询SQL
     *
     * @param fieldMap
     * @param queryOperator and/or
     * @param filter
     * @return
     */
    private String buildFilterSql(Map<String, Field> fieldMap, String queryOperator, List<Filter> filter) {
        List<Filter> list = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), queryOperator)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        int size = list.size();
        int end = size - 1;
        StringBuilder sql = new StringBuilder();
        sql.append("(");
        Filter c = null;
        String quotation = buildSqlWithQuotation();
        for (int i = 0; i < size; i++) {
            c = list.get(i);
            Field field = fieldMap.get(c.getName());
            Assert.notNull(field, "条件字段无效.");
            // "USER" = 'zhangsan'
            sql.append(quotation);
            sql.append(buildFieldName(field));
            sql.append(quotation);
            sql.append(" ").append(c.getFilter()).append(" ");
            // 如果使用了函数则不加引号
            sql.append(buildFilterValue(c.getValue()));
            if (i < end) {
                sql.append(" ").append(queryOperator).append(" ");
            }
        }
        sql.append(")");
        return sql.toString();
    }

    /**
     * 获取条件值引号（如包含系统表达式则排除引号）
     *
     * @param value
     * @return
     */
    protected String buildFilterValue(String value) {
        if (StringUtil.isNotBlank(value)) {
            // 系统函数表达式 $select max(update_time)$
            Matcher matcher = Pattern.compile(SYS_EXPRESSION).matcher(value);
            if (matcher.find()) {
                return StringUtil.substring(value, 1, value.length() - 1);
            }
        }
        return new StringBuilder(StringUtil.SINGLE_QUOTATION).append(value).append(StringUtil.SINGLE_QUOTATION).toString();
    }

    /**
     * 生成SQL
     *
     * @param map
     * @param builderType
     * @param config
     */
    private void buildSql(Map<String, String> map, SqlBuilderEnum builderType, SqlBuilderConfig config) {
        map.put(builderType.getName(), builderType.getSqlBuilder().buildSql(config));
    }

    /**
     * 返回表主键
     *
     * @param md
     * @param catalog
     * @param schema
     * @param tableName
     * @return
     * @throws SQLException
     */
    private List<String> findTablePrimaryKeys(DatabaseMetaData md, String catalog, String schema, String tableName) throws SQLException {
        //根据表名获得主键结果集
        ResultSet rs = null;
        List<String> primaryKeys = new ArrayList<>();
        try {
            rs = md.getPrimaryKeys(catalog, schema, tableName);
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            DatabaseUtil.close(rs);
        }
        return primaryKeys;
    }

    private List<Object[]> batchRows(List<Field> fields, List<Map> data) {
        return data.stream().map(row -> batchRow(fields, row)).collect(Collectors.toList());
    }

    private Object[] batchRow(List<Field> fields, Map row) {
        final int size = fields.size();
        Object[] args = new Object[size];
        for (int i = 0; i < size; i++) {
            args[i] = row.get(fields.get(i).getName());
        }
        return args;
    }

    private void forceUpdate(Result result, DatabaseConnectorInstance connectorMapper, WriterBatchConfig config, List<Field> pkFields,
                             Map row) {
        if (isUpdate(config.getEvent()) || isInsert(config.getEvent())) {
            // 存在执行覆盖更新，否则写入
            final String queryCount = config.getCommand().get(ConnectorConstant.OPERTION_QUERY_EXIST);
            int size = pkFields.size();
            Object[] args = new Object[size];
            for (int i = 0; i < size; i++) {
                args[i] = row.get(pkFields.get(i).getName());
            }
            final String event = existRow(connectorMapper, queryCount, args) ? ConnectorConstant.OPERTION_UPDATE
                    : ConnectorConstant.OPERTION_INSERT;
            logger.warn("{}表执行{}失败, 重新执行{}, {}", config.getTableName(), config.getEvent(), event, row);
            writer(result, connectorMapper, config, pkFields, row, event);
        }
    }

    private void writer(Result result, DatabaseConnectorInstance connectorMapper, WriterBatchConfig config, List<Field> pkFields, Map row,
                        String event) {
        // 1、获取 SQL
        String sql = config.getCommand().get(event);

        List<Field> fields = new ArrayList<>(config.getFields());
        // Update / Delete
        if (!isInsert(event)) {
            if (isDelete(event)) {
                fields.clear();
            } else if (isUpdate(event)) {
                removeFieldWithPk(fields, pkFields);
            }
            fields.addAll(pkFields);
        }

        try {
            // 2、设置参数
            int execute = connectorMapper.execute(databaseTemplate -> databaseTemplate.update(sql, batchRow(fields, row)));
            if (execute == 0) {
                throw new SdkException(String.format("尝试执行[%s]失败", event));
            }
            result.getSuccessData().add(row);
        } catch (Exception e) {
            result.getFailData().add(row);
            result.getError().append("SQL:").append(sql).append(System.lineSeparator())
                    .append("DATA:").append(row).append(System.lineSeparator())
                    .append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
            logger.error("执行{}失败: {}, DATA:{}", event, e.getMessage(), row);
        }
    }

    private boolean existRow(DatabaseConnectorInstance connectorMapper, String sql, Object[] args) {
        int rowNum = 0;
        try {
            rowNum = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(sql, Integer.class, args));
        } catch (Exception e) {
            logger.error("检查数据行存在异常:{}，SQL:{},参数:{}", e.getMessage(), sql, args);
        }
        return rowNum > 0;
    }

    private boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        return !CollectionUtils.isEmpty(pk) && pk.contains(name);
    }

    private void removeFieldWithPk(List<Field> fields, List<Field> pkFields) {
        if (CollectionUtils.isEmpty(fields) || CollectionUtils.isEmpty(pkFields)) {
            return;
        }

        pkFields.forEach(pkField -> {
            Iterator<Field> iterator = fields.iterator();
            while (iterator.hasNext()) {
                Field next = iterator.next();
                if (next != null && StringUtil.equals(next.getName(), pkField.getName())) {
                    iterator.remove();
                    break;
                }
            }
        });
    }

    @Override
    public Result writerDDL(DatabaseConnectorInstance connectorMapper, DDLConfig config) {
        Result result = new Result();
        try {
            Assert.hasText(config.getSql(), "执行SQL语句不能为空.");
            connectorMapper.execute(databaseTemplate -> {
                databaseTemplate.execute(config.getSql());
                return true;
            });
        } catch (Exception e) {
            result.getError().append(String.format("执行ddl: %s, 异常：%s", config.getSql(), e.getMessage()));
        }
        return result;
    }
}