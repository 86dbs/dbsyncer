/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
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
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 关系型数据库连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public abstract class AbstractDatabaseConnector extends AbstractConnector implements ConnectorService<DatabaseConnectorInstance, DatabaseConfig>, Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Class<DatabaseConfig> getConfigClass() {
        return DatabaseConfig.class;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config) {
        return new DatabaseConnectorInstance(config);
    }

    @Override
    public void disconnect(DatabaseConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(DatabaseConnectorInstance connectorInstance) {
        Integer count = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject(getValidationQuery(), Integer.class));
        return null != count && count > 0;
    }

    @Override
    public String getConnectorInstanceCacheKey(DatabaseConfig config) {
        return String.format("%s-%s-%s", config.getConnectorType(), config.getUrl(), config.getUsername());
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance) {
        return getTable(connectorInstance, null, getSchema(connectorInstance.getConfig()), null);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorInstance connectorInstance, String tableNamePattern) {
        List<Field> fields = new ArrayList<>();
        final String schema = getSchema(connectorInstance.getConfig());
        connectorInstance.execute(databaseTemplate -> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            String catalog = conn.getCatalog();
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            DatabaseMetaData metaData = conn.getMetaData();
            List<String> primaryKeys = findTablePrimaryKeys(metaData, catalog, schemaNamePattern, tableNamePattern);
            try (ResultSet columnMetadata = metaData.getColumns(catalog, schemaNamePattern, tableNamePattern, null)) {
                while (columnMetadata.next()) {
                    String columnName = columnMetadata.getString("COLUMN_NAME");
                    int columnType = columnMetadata.getInt("DATA_TYPE");
                    String typeName = columnMetadata.getString("TYPE_NAME");
                    int columnSize = Math.max(0, columnMetadata.getInt("COLUMN_SIZE"));
                    int ratio = Math.max(0, columnMetadata.getInt("DECIMAL_DIGITS"));
                    fields.add(new Field(columnName, typeName, columnType, primaryKeys.contains(columnName), columnSize, ratio));
                }
            }
            return fields;
        });
        return new MetaInfo().setColumn(fields);
    }

    @Override
    public long getCount(DatabaseConnectorInstance connectorInstance, Map<String, String> command) {
        if (CollectionUtils.isEmpty(command)) {
            return 0L;
        }

        // 1、获取select SQL
        String queryCountSql = command.get(ConnectorConstant.OPERTION_QUERY_COUNT);
        if (StringUtil.isBlank(queryCountSql)) {
            return 0;
        }

        // 2、返回结果集
        try {
            return connectorInstance.execute(databaseTemplate -> {
                Long count = databaseTemplate.queryForObject(queryCountSql, Long.class);
                return count == null ? 0 : count;
            });
        } catch (EmptyResultDataAccessException e) {
            return 0;
        }
    }

    @Override
    public Result reader(DatabaseConnectorInstance connectorInstance, ReaderContext context) {
        // 1、获取select SQL
        boolean supportedCursor = enableCursor() && context.isSupportedCursor() && null != context.getCursors();
        String queryKey = supportedCursor ? ConnectorConstant.OPERTION_QUERY_CURSOR : ConnectorConstant.OPERTION_QUERY;
        final String querySql = context.getCommand().get(queryKey);
        Assert.hasText(querySql, "查询语句不能为空.");

        // 2、设置参数
        Collections.addAll(context.getArgs(), supportedCursor ? getPageCursorArgs(context) : getPageArgs(context));

        // 3、执行SQL
        List<Map<String, Object>> list = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, context.getArgs().toArray()));

        // 4、返回结果集
        return new Result(list);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        String event = context.getEvent();
        List<Map> data = context.getTargetList();
        List<Field> targetFields = context.getTargetFields();

        // 1、获取SQL
        String executeSql = context.getCommand().get(event);
        Assert.hasText(executeSql, "执行SQL语句不能为空.");
        if (CollectionUtils.isEmpty(targetFields)) {
            logger.error("writer fields can not be empty.");
            throw new SdkException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new SdkException("writer data can not be empty.");
        }
        List<Field> fields = new ArrayList<>(targetFields);
        List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(targetFields);
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
            execute = connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(executeSql, batchRows(fields, data)));
        } catch (Exception e) {
            if (context.isForceUpdate()) {
                data.forEach(row -> forceUpdate(result, connectorInstance, context, pkFields, row));
            }
        }

        if (null != execute) {
            int batchSize = execute.length;
            for (int i = 0; i < batchSize; i++) {
                if (execute[i] == 1 || execute[i] == -2) {
                    result.getSuccessData().add(data.get(i));
                    continue;
                }
                if (context.isForceUpdate()) {
                    forceUpdate(result, connectorInstance, context, pkFields, data.get(i));
                }
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
     * 满足游标查询条件，追加主键排序（单个主键才做排序）
     *
     * @param config
     * @param sql
     */
    protected void appendOrderByPk(PageSql config, StringBuilder sql) {
        if (!CollectionUtils.isEmpty(config.getPrimaryKeys()) && config.getPrimaryKeys().size() == 1) {
            sql.append(" ORDER BY ");
            final String quotation = buildSqlWithQuotation();
            PrimaryKeyUtil.buildSql(sql, config.getPrimaryKeys(), quotation, ",", "", true);
        }
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

        // 自定义SQL
        Optional<Filter> sqlFilter = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), OperationEnum.SQL.getName())).findFirst();
        sqlFilter.ifPresent(f -> sql.append(f.getValue()));

        // 如果有条件加上 WHERE
        if (StringUtil.isNotBlank(sql.toString())) {
            // WHERE (USER.USERNAME = 'zhangsan' AND USER.AGE='20') OR (USER.TEL='18299996666')
            sql.insert(0, " WHERE ");
        }
        return sql.toString();
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
    private String getQueryCountSql(CommandConfig commandConfig, List<String> primaryKeys, String schema, String queryFilterSql) {
        Table table = commandConfig.getTable();
        String tableName = table.getName();
        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, table.getColumn(), queryFilterSql);
        return SqlBuilderEnum.QUERY_COUNT.getSqlBuilder().buildSql(config);
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
     * @param connectorInstance
     * @param catalog
     * @param schema
     * @param tableNamePattern
     * @return
     */
    private List<Table> getTable(DatabaseConnectorInstance connectorInstance, String catalog, String schema, String tableNamePattern) {
        return connectorInstance.execute(databaseTemplate -> {
            List<Table> tables = new ArrayList<>();
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            // 兼容处理 schema 和 catalog
            String effectiveCatalog = null;
            String effectiveSchema = null;
            if (dbProductName.contains("mysql") || dbProductName.contains("mariadb")) {
                // MySQL: schema=null, catalog=database name
                effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
            } else if (dbProductName.contains("oracle")) {
                // Oracle: schema=用户名（大写），catalog=null
                effectiveSchema = (schema != null) ? schema : conn.getSchema();
                if (effectiveSchema != null) {
                    effectiveSchema = effectiveSchema.toUpperCase();
                }
            } else if (dbProductName.contains("postgresql")) {
                // PostgreSQL: schema=public 等，catalog=数据库名
                effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
                effectiveSchema = (schema != null) ? schema : "public";
            } else if (dbProductName.contains("sql server")) {
                // SQL Server: catalog=数据库名，schema=如 dbo
                effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
                effectiveSchema = (schema != null) ? schema : "dbo";
            } else {
                // 其他数据库按默认处理
                effectiveCatalog = (catalog != null) ? catalog : conn.getCatalog();
                effectiveSchema = (schema != null) ? schema : conn.getSchema();
            }

            String[] types = {TableTypeEnum.TABLE.getCode(), TableTypeEnum.VIEW.getCode(), TableTypeEnum.MATERIALIZED_VIEW.getCode()};
            final ResultSet rs = conn.getMetaData().getTables(effectiveCatalog, effectiveSchema, tableNamePattern, types);
            logger.info("Using dbProductName：{}, catalog: {}, schema: {}", dbProductName, effectiveCatalog, effectiveSchema);
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
     * @param queryOperator {@link OperationEnum}
     * @param filter
     * @return
     */
    private String buildFilterSql(Map<String, Field> fieldMap, String operator, List<Filter> filter) {
        List<Filter> list = filter.stream().filter(f -> StringUtil.equals(f.getOperation(), operator)).collect(Collectors.toList());
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
            // 如果使用了函数则不加引号
            FilterEnum filterEnum = FilterEnum.getFilterEnum(c.getFilter());
            switch (filterEnum) {
                case IN:
                    sql.append(StringUtil.SPACE).append(FilterEnum.IN.getName()).append(StringUtil.SPACE).append("(");
                    sql.append(c.getValue());
                    sql.append(")");
                    break;
                case IS_NULL:
                case IS_NOT_NULL:
                    sql.append(StringUtil.SPACE).append(filterEnum.getName().toUpperCase()).append(StringUtil.SPACE);
                    break;
                default:
                    // > 10
                    sql.append(StringUtil.SPACE).append(c.getFilter()).append(StringUtil.SPACE);
                    sql.append(buildFilterValue(c.getValue()));
                    break;
            }
            if (i < end) {
                sql.append(StringUtil.SPACE).append(operator).append(StringUtil.SPACE);
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
    private String buildFilterValue(String value) {
        if (StringUtil.isNotBlank(value)) {
            // 排除定时表达式
            if (QuartzFilterEnum.getQuartzFilterEnum(value) == null) {
                // 系统函数表达式 $select max(update_time)$
                Matcher matcher = Pattern.compile("^[$].*[$]$").matcher(value);
                if (matcher.find()) {
                    return StringUtil.substring(value, 1, value.length() - 1);
                }
            }
        }
        return StringUtil.SINGLE_QUOTATION + value + StringUtil.SINGLE_QUOTATION;
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

    private void forceUpdate(Result result, DatabaseConnectorInstance connectorInstance, PluginContext context, List<Field> pkFields,
                             Map row) {
        if (isUpdate(context.getEvent()) || isInsert(context.getEvent())) {
            // 存在执行覆盖更新，否则写入
            final String queryCount = context.getCommand().get(ConnectorConstant.OPERTION_QUERY_EXIST);
            int size = pkFields.size();
            Object[] args = new Object[size];
            for (int i = 0; i < size; i++) {
                args[i] = row.get(pkFields.get(i).getName());
            }
            final String event = existRow(connectorInstance, queryCount, args) ? ConnectorConstant.OPERTION_UPDATE
                    : ConnectorConstant.OPERTION_INSERT;
            logger.warn("{} {}表执行{}失败, 重新执行{}, {}", context.getTraceId(), context.getTargetTableName(), context.getEvent(), event, row);
            writer(result, connectorInstance, context, pkFields, row, event);
        }
    }

    private void writer(Result result, DatabaseConnectorInstance connectorInstance, PluginContext context, List<Field> pkFields, Map row,
                        String event) {
        // 1、获取 SQL
        String sql = context.getCommand().get(event);

        List<Field> fields = new ArrayList<>(context.getTargetFields());
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
            int execute = connectorInstance.execute(databaseTemplate -> databaseTemplate.update(sql, batchRow(fields, row)));
            if (execute == 0) {
                throw new SdkException(String.format("%s 尝试执行[%s]失败", context.getTraceId(), event));
            }
            result.getSuccessData().add(row);
        } catch (Exception e) {
            result.getFailData().add(row);
            result.getError().append(context.getTraceId())
                    .append(" SQL:").append(sql).append(System.lineSeparator())
                    .append("DATA:").append(row).append(System.lineSeparator())
                    .append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
            logger.error("执行{}失败: {}, DATA:{}", event, e.getMessage(), row);
        }
    }

    private boolean existRow(DatabaseConnectorInstance connectorInstance, String sql, Object[] args) {
        int rowNum = 0;
        try {
            rowNum = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForObject(sql, Integer.class, args));
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
    public Result writerDDL(DatabaseConnectorInstance connectorInstance, DDLConfig config) {
        Result result = new Result();
        try {
            Assert.hasText(config.getSql(), "执行SQL语句不能为空.");
            connectorInstance.execute(databaseTemplate -> {
                // 执行ddl时, 带上dbs唯一标识码，防止双向同步导致死循环
                databaseTemplate.execute(DatabaseConstant.DBS_UNIQUE_CODE.concat(config.getSql()));
                return true;
            });
            Map<String, String> successMap = new HashMap<>();
            successMap.put(ConfigConstant.BINLOG_DATA, config.getSql());
            result.addSuccessData(Collections.singletonList(successMap));
        } catch (Exception e) {
            result.getError().append(String.format("执行ddl: %s, 异常：%s", config.getSql(), e.getMessage()));
        }
        return result;
    }
}