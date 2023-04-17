package org.dbsyncer.connector.database;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.PageSql;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
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
import java.util.stream.Collectors;

public abstract class AbstractDatabaseConnector extends AbstractConnector implements Connector<DatabaseConnectorMapper, DatabaseConfig>, Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectorMapper connect(DatabaseConfig config) {
        try {
            return new DatabaseConnectorMapper(config);
        } catch (Exception e) {
            logger.error("Failed to connect:{}, message:{}", config.getUrl(), e.getMessage());
        }
        throw new ConnectorException(String.format("Failed to connect:%s", config.getUrl()));
    }

    @Override
    public void disconnect(DatabaseConnectorMapper connectorMapper) {
        connectorMapper.close();
    }

    @Override
    public boolean isAlive(DatabaseConnectorMapper connectorMapper) {
        Integer count = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(getValidationQuery(), Integer.class));
        return null != count && count > 0;
    }

    @Override
    public String getConnectorMapperCacheKey(DatabaseConfig config) {
        return String.format("%s-%s-%s", config.getConnectorType(), config.getUrl(), config.getUsername());
    }

    @Override
    public List<Table> getTable(DatabaseConnectorMapper connectorMapper) {
        return getTable(connectorMapper, null, getSchema(connectorMapper.getConfig()), null);
    }

    @Override
    public MetaInfo getMetaInfo(DatabaseConnectorMapper connectorMapper, String tableNamePattern) {
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
    public long getCount(DatabaseConnectorMapper connectorMapper, Map<String, String> command) {
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
    public Result reader(DatabaseConnectorMapper connectorMapper, ReaderConfig config) {
        // 1、获取select SQL
        boolean supportedCursor = enableCursor() && config.isSupportedCursor() && null != config.getCursors();
        String queryKey = supportedCursor ? ConnectorConstant.OPERTION_QUERY_CURSOR : ConnectorConstant.OPERTION_QUERY;
        String querySql = config.getCommand().get(queryKey);
        Assert.hasText(querySql, "查询语句不能为空.");

        // 2、设置参数
        Collections.addAll(config.getArgs(), config.isSupportedCursor() ? getPageCursorArgs(config) : getPageArgs(config));

        // 3、执行SQL
        List<Map<String, Object>> list = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, config.getArgs().toArray()));

        // 4、返回结果集
        return new Result(list);
    }

    @Override
    public Result writer(DatabaseConnectorMapper connectorMapper, WriterBatchConfig config) {
        String event = config.getEvent();
        List<Map> data = config.getData();

        // 1、获取SQL
        String executeSql = config.getCommand().get(event);
        Assert.hasText(executeSql, "执行SQL语句不能为空.");
        if (CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer fields can not be empty.");
            throw new ConnectorException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }
        List<Field> fields = new ArrayList<>(config.getFields());
        List<Field> pkFields = getPrimaryKeys(config.getFields());
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
            logger.error(e.getMessage());
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
        // 获取过滤SQL
        final String queryFilterSql = getQueryFilterSql(commandConfig.getFilter());
        final String quotation = buildSqlWithQuotation();

        // 获取查询SQL
        Map<String, String> map = new HashMap<>();
        String schema = getSchema((DatabaseConfig) commandConfig.getConnectorConfig(), quotation);
        map.put(ConnectorConstant.OPERTION_QUERY, buildSql(ConnectorConstant.OPERTION_QUERY, commandConfig, schema, queryFilterSql));
        // 是否支持游标
        if (enableCursor()) {
            map.put(ConnectorConstant.OPERTION_QUERY_CURSOR, buildSql(ConnectorConstant.OPERTION_QUERY_CURSOR, commandConfig, schema, queryFilterSql));
        }
        // 获取查询总数SQL
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, getQueryCountSql(commandConfig, schema, quotation, queryFilterSql));
        return map;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        String quotation = buildSqlWithQuotation();
        String schema = getSchema((DatabaseConfig) commandConfig.getConnectorConfig(), quotation);

        // 获取增删改SQL
        Map<String, String> map = new HashMap<>();
        String insert = SqlBuilderEnum.INSERT.getName();
        map.put(insert, buildSql(insert, commandConfig, schema, null));

        String update = SqlBuilderEnum.UPDATE.getName();
        map.put(update, buildSql(update, commandConfig, schema, null));

        String delete = SqlBuilderEnum.DELETE.getName();
        map.put(delete, buildSql(delete, commandConfig, schema, null));

        // 获取查询数据行是否存在
        String tableName = commandConfig.getTable().getName();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(commandConfig.getTable());
        if (!CollectionUtils.isEmpty(primaryKeys)) {
            StringBuilder queryCount = new StringBuilder("SELECT COUNT(1) FROM ").append(schema).append(quotation).append(tableName).append(quotation).append(" WHERE ");
            // id = ? AND uid = ?
            PrimaryKeyUtil.buildSql(queryCount, primaryKeys, quotation, " AND ", " = ? ", true);

            String queryCountExist = ConnectorConstant.OPERTION_QUERY_COUNT_EXIST;
            map.put(queryCountExist, queryCount.toString());
        }
        return map;
    }

    /**
     * 查询语句表名和字段带上引号（默认不加）
     *
     * @return
     */
    public String buildSqlWithQuotation() {
        return "";
    }

    /**
     * 是否使用游标查询
     *
     * @return
     */
    protected boolean enableCursor() {
        return false;
    }

    /**
     * 是否支持游标配置
     *
     * @param config
     * @return
     */
    protected boolean isSupportedCursor(PageSql config) {
        final List<String> primaryKeys = config.getPrimaryKeys();
        final SqlBuilderConfig sqlBuilderConfig = config.getSqlBuilderConfig();
        final Map<String, Integer> typeAliases = PrimaryKeyUtil.findPrimaryKeyType(sqlBuilderConfig.getFields());
        return PrimaryKeyUtil.isSupportedCursor(typeAliases, primaryKeys);
    }

    /**
     * 如果满足游标，追加主键排序
     *
     * @param config
     * @param sql
     */
    protected void appendOrderByPkIfSupportedCursor(PageSql config, StringBuilder sql) {
        if (isSupportedCursor(config)) {
            sql.append(" ORDER BY ");
            final List<String> primaryKeys = config.getPrimaryKeys();
            final String quotation = config.getQuotation();
            PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        }
    }

    /**
     * 健康检查
     *
     * @return
     */
    protected String getValidationQuery() {
        return "select 1";
    }

    /**
     * 获取条件值引号
     *
     * @param value
     * @return
     */
    protected String buildSqlFilterWithQuotation(String value) {
        return "'";
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
            throw new ConnectorException("查询表字段不能为空.");
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
     * @param schema
     * @param quotation
     * @param queryFilterSql
     * @return
     */
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        String table = commandConfig.getTable().getName();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(commandConfig.getTable());
        StringBuilder sql = new StringBuilder();
        if (!CollectionUtils.isEmpty(primaryKeys)) {
            sql.append("SELECT COUNT(1) FROM (SELECT 1 FROM ").append(schema).append(quotation).append(table).append(quotation);
            if (StringUtil.isNotBlank(queryFilterSql)) {
                sql.append(queryFilterSql);
            }
            sql.append(" GROUP BY ");
            // id,uid
            PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
            sql.append(") DBSYNCER_T");
        }
        return sql.toString();
    }

    /**
     * 获取查询条件SQL
     *
     * @param filter
     * @return
     */
    protected String getQueryFilterSql(List<Filter> filter) {
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        // 过滤条件SQL
        StringBuilder sql = new StringBuilder();

        // 拼接并且SQL
        String addSql = getFilterSql(OperationEnum.AND.getName(), filter);
        // 如果Add条件存在
        if (StringUtil.isNotBlank(addSql)) {
            sql.append(addSql);
        }

        // 拼接或者SQL
        String orSql = getFilterSql(OperationEnum.OR.getName(), filter);
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
     * 获取架构名
     *
     * @param config
     * @param quotation
     * @return
     */
    protected String getSchema(DatabaseConfig config, String quotation) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            schema.append(quotation).append(config.getSchema()).append(quotation).append(".");
        }
        return schema.toString();
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
    private List<Table> getTable(DatabaseConnectorMapper connectorMapper, String catalog, String schema, String tableNamePattern) {
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
     * @param queryOperator and/or
     * @param filter
     * @return
     */
    private String getFilterSql(String queryOperator, List<Filter> filter) {
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
            // "USER" = 'zhangsan'
            sql.append(quotation).append(c.getName()).append(quotation);
            sql.append(" ").append(c.getFilter()).append(" ");
            // 如果使用了函数则不加引号
            String filterValueQuotation = buildSqlFilterWithQuotation(c.getValue());
            sql.append(filterValueQuotation).append(c.getValue()).append(filterValueQuotation);
            if (i < end) {
                sql.append(" ").append(queryOperator).append(" ");
            }
        }
        sql.append(")");
        return sql.toString();
    }

    /**
     * 获取查询SQL
     *
     * @param type           {@link SqlBuilderEnum}
     * @param commandConfig
     * @param schema
     * @param queryFilterSQL
     * @return
     */
    private String buildSql(String type, CommandConfig commandConfig, String schema, String queryFilterSQL) {
        Table table = commandConfig.getTable();
        if (null == table) {
            logger.error("Table can not be null.");
            throw new ConnectorException("Table can not be null.");
        }
        List<Field> column = table.getColumn();
        if (CollectionUtils.isEmpty(column)) {
            return null;
        }
        Set<String> mark = new HashSet<>();
        List<Field> fields = new ArrayList<>();
        for (Field c : column) {
            String name = c.getName();
            if (StringUtil.isBlank(name)) {
                throw new ConnectorException("The field name can not be empty.");
            }
            if (!mark.contains(name)) {
                fields.add(c);
                mark.add(name);
            }
        }
        if (CollectionUtils.isEmpty(fields)) {
            logger.error("The fields can not be empty.");
            throw new ConnectorException("The fields can not be empty.");
        }
        String tableName = table.getName();
        if (StringUtil.isBlank(tableName)) {
            logger.error("Table name can not be empty.");
            throw new ConnectorException("Table name can not be empty.");
        }
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(commandConfig.getTable());
        if (!CollectionUtils.isEmpty(primaryKeys)) {
            SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, fields, queryFilterSQL, buildSqlWithQuotation());
            return SqlBuilderEnum.getSqlBuilder(type).buildSql(config);
        }
        return "";
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

    private void forceUpdate(Result result, DatabaseConnectorMapper connectorMapper, WriterBatchConfig config, List<Field> pkFields,
                             Map row) {
        if (isUpdate(config.getEvent()) || isInsert(config.getEvent())) {
            // 存在执行覆盖更新，否则写入
            final String queryCount = config.getCommand().get(ConnectorConstant.OPERTION_QUERY_COUNT_EXIST);
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

    private void writer(Result result, DatabaseConnectorMapper connectorMapper, WriterBatchConfig config, List<Field> pkFields, Map row,
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
                throw new ConnectorException(String.format("尝试执行[%s]失败", event));
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

    private boolean existRow(DatabaseConnectorMapper connectorMapper, String sql, Object[] args) {
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

}