package org.dbsyncer.connector.database;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.connector.model.MetaInfo;
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
import java.util.*;
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
            SimpleConnection connection = (SimpleConnection) databaseTemplate.getConnection();
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

        // 1?????????select SQL
        String queryCountSql = command.get(ConnectorConstant.OPERTION_QUERY_COUNT);
        Assert.hasText(queryCountSql, "??????????????????????????????.");

        // 2??????????????????
        return connectorMapper.execute(databaseTemplate -> {
            Long count = databaseTemplate.queryForObject(queryCountSql, Long.class);
            return count == null ? 0 : count;
        });
    }

    @Override
    public Result reader(DatabaseConnectorMapper connectorMapper, ReaderConfig config) {
        // 1?????????select SQL
        String queryKey = enableCursor() && null == config.getCursor() ? ConnectorConstant.OPERTION_QUERY_CURSOR : SqlBuilderEnum.QUERY.getName();
        String querySql = config.getCommand().get(queryKey);
        Assert.hasText(querySql, "????????????????????????.");

        // 2???????????????
        Collections.addAll(config.getArgs(), getPageArgs(config));

        // 3?????????SQL
        List<Map<String, Object>> list = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, config.getArgs().toArray()));

        // 4??????????????????
        return new Result(list);
    }

    @Override
    public Result writer(DatabaseConnectorMapper connectorMapper, WriterBatchConfig config) {
        String event = config.getEvent();
        List<Map> data = config.getData();

        // 1?????????SQL
        String executeSql = config.getCommand().get(event);
        Assert.hasText(executeSql, "??????SQL??????????????????.");
        if (CollectionUtils.isEmpty(config.getFields())) {
            logger.error("writer fields can not be empty.");
            throw new ConnectorException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }
        List<Field> fields = new ArrayList<>(config.getFields());
        Field pkField = getPrimaryKeyField(config.getFields());
        // Update / Delete
        if (!isInsert(event)) {
            if (isDelete(event)) {
                fields.clear();
            } else if (isUpdate(event)) {
                fields.remove(pkField);
            }
            fields.add(pkField);
        }

        Result result = new Result();
        int[] execute = null;
        try {
            // 2???????????????
            execute = connectorMapper.execute(databaseTemplate -> databaseTemplate.batchUpdate(executeSql, batchRows(fields, data)));
        } catch (Exception e) {
            logger.error(e.getMessage());
            data.forEach(row -> forceUpdate(result, connectorMapper, config, pkField, row));
        }

        if (null != execute) {
            int batchSize = execute.length;
            for (int i = 0; i < batchSize; i++) {
                if (execute[i] == 1 || execute[i] == -2) {
                    result.getSuccessData().add(data.get(i));
                    continue;
                }
                forceUpdate(result, connectorMapper, config, pkField, data.get(i));
            }
        }
        return result;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        // ????????????SQL
        final String queryFilterSql = getQueryFilterSql(commandConfig.getFilter());
        final String quotation = buildSqlWithQuotation();

        // ????????????SQL
        Map<String, String> map = new HashMap<>();
        String schema = getSchema((DatabaseConfig) commandConfig.getConnectorConfig(), quotation);
        map.put(ConnectorConstant.OPERTION_QUERY, buildSql(ConnectorConstant.OPERTION_QUERY, commandConfig, schema, queryFilterSql));
        // ??????????????????
        if (enableCursor()) {
            map.put(ConnectorConstant.OPERTION_QUERY_CURSOR, buildSql(ConnectorConstant.OPERTION_QUERY_CURSOR, commandConfig, schema, queryFilterSql));
        }
        // ??????????????????SQL
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, getQueryCountSql(commandConfig, schema, quotation, queryFilterSql));
        return map;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        String quotation = buildSqlWithQuotation();
        String schema = getSchema((DatabaseConfig) commandConfig.getConnectorConfig(), quotation);

        // ???????????????SQL
        Map<String, String> map = new HashMap<>();
        String insert = SqlBuilderEnum.INSERT.getName();
        map.put(insert, buildSql(insert, commandConfig, schema, null));

        String update = SqlBuilderEnum.UPDATE.getName();
        map.put(update, buildSql(update, commandConfig, schema, null));

        String delete = SqlBuilderEnum.DELETE.getName();
        map.put(delete, buildSql(delete, commandConfig, schema, null));

        // ?????????????????????????????????
        String tableName = commandConfig.getTable().getName();
        String pk = PrimaryKeyUtil.findOriginalTablePrimaryKey(commandConfig.getTable());
        StringBuilder queryCount = new StringBuilder("SELECT COUNT(1) FROM ").append(schema).append(quotation).append(tableName).append(quotation)
                .append(" WHERE ").append(quotation).append(pk).append(quotation).append(" = ?");
        String queryCountExist = ConnectorConstant.OPERTION_QUERY_COUNT_EXIST;
        map.put(queryCountExist, queryCount.toString());
        return map;
    }

    /**
     * ????????????????????????
     *
     * @return
     */
    protected boolean enableCursor() {
        return false;
    }

    /**
     * ????????????
     *
     * @return
     */
    protected String getValidationQuery() {
        return "select 1";
    }

    /**
     * ?????????????????????????????????????????????????????????
     *
     * @return
     */
    protected String buildSqlWithQuotation() {
        return "";
    }

    /**
     * ?????????????????????
     *
     * @param value
     * @return
     */
    protected String buildSqlFilterWithQuotation(String value) {
        return "'";
    }

    /**
     * ???????????????
     *
     * @param config
     * @return
     */
    protected String getSchema(DatabaseConfig config) {
        return config.getSchema();
    }

    /**
     * ???????????????
     *
     * @param connectorMapper
     * @param catalog
     * @param schema
     * @param tableNamePattern
     * @return
     */
    protected List<Table> getTable(DatabaseConnectorMapper connectorMapper, String catalog, String schema, String tableNamePattern) {
        return connectorMapper.execute(databaseTemplate -> {
            List<Table> tables = new ArrayList<>();
            SimpleConnection connection = (SimpleConnection) databaseTemplate.getConnection();
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
     * ?????????????????????????????????
     *
     * @param databaseTemplate
     * @param metaSql          ???????????????
     * @param schema           ?????????
     * @param tableName        ??????
     * @return
     */
    protected MetaInfo getMetaInfo(DatabaseTemplate databaseTemplate, String metaSql, String schema, String tableName) throws SQLException {
        SqlRowSet sqlRowSet = databaseTemplate.queryForRowSet(metaSql);
        ResultSetWrappingSqlRowSet rowSet = (ResultSetWrappingSqlRowSet) sqlRowSet;
        SqlRowSetMetaData metaData = rowSet.getMetaData();

        // ?????????????????????
        int columnCount = metaData.getColumnCount();
        if (1 > columnCount) {
            throw new ConnectorException("???????????????????????????.");
        }
        List<Field> fields = new ArrayList<>(columnCount);
        Map<String, List<String>> tables = new HashMap<>();
        try {
            Connection connection = databaseTemplate.getConnection();
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
     * ??????????????????SQL
     *
     * @param commandConfig
     * @param schema
     * @param quotation
     * @param queryFilterSql
     * @return
     */
    protected String getQueryCountSql(CommandConfig commandConfig, String schema, String quotation, String queryFilterSql) {
        String table = commandConfig.getTable().getName();
        String pk = PrimaryKeyUtil.findOriginalTablePrimaryKey(commandConfig.getTable());
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (SELECT 1 FROM ").append(schema).append(quotation).append(table).append(quotation);
        if (StringUtil.isNotBlank(queryFilterSql)) {
            queryCount.append(queryFilterSql);
        }
        queryCount.append(" GROUP BY ").append(quotation).append(pk).append(quotation).append(") DBSYNCER_T");
        return queryCount.toString();
    }

    /**
     * ??????????????????SQL
     *
     * @param filter
     * @return
     */
    protected String getQueryFilterSql(List<Filter> filter) {
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        // ????????????SQL
        StringBuilder sql = new StringBuilder();

        // ????????????SQL
        String addSql = getFilterSql(OperationEnum.AND.getName(), filter);
        // ??????Add????????????
        if (StringUtil.isNotBlank(addSql)) {
            sql.append(addSql);
        }

        // ????????????SQL
        String orSql = getFilterSql(OperationEnum.OR.getName(), filter);
        // ??????Or?????????Add???????????????
        if (StringUtil.isNotBlank(orSql) && StringUtil.isNotBlank(addSql)) {
            sql.append(" OR ");
        }
        sql.append(orSql);

        // ????????????????????? WHERE
        if (StringUtil.isNotBlank(sql.toString())) {
            // WHERE (USER.USERNAME = 'zhangsan' AND USER.AGE='20') OR (USER.TEL='18299996666')
            sql.insert(0, " WHERE ");
        }
        return sql.toString();
    }

    /**
     * ??????????????????????????????SQL
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
            // ????????????????????????????????????
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
     * ????????????SQL
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
        String pk = null;
        Set<String> mark = new HashSet<>();
        List<Field> fields = new ArrayList<>();
        for (Field c : column) {
            String name = c.getName();
            if (StringUtil.isBlank(name)) {
                throw new ConnectorException("The field name can not be empty.");
            }
            if (c.isPk()) {
                pk = name;
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
        if (StringUtil.isBlank(pk)) {
            pk = PrimaryKeyUtil.findOriginalTablePrimaryKey(table);
        }

        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, pk, fields, queryFilterSQL, buildSqlWithQuotation());
        return SqlBuilderEnum.getSqlBuilder(type).buildSql(config);
    }

    /**
     * ???????????????
     *
     * @param config
     * @param quotation
     * @return
     */
    private String getSchema(DatabaseConfig config, String quotation) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            schema.append(quotation).append(config.getSchema()).append(quotation).append(".");
        }
        return schema.toString();
    }

    /**
     * ???????????????
     *
     * @param md
     * @param catalog
     * @param schema
     * @param tableName
     * @return
     * @throws SQLException
     */
    private List<String> findTablePrimaryKeys(DatabaseMetaData md, String catalog, String schema, String tableName) throws SQLException {
        //?????????????????????????????????
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

    private void forceUpdate(Result result, DatabaseConnectorMapper connectorMapper, WriterBatchConfig config, Field pkField,
                             Map row) {
        if(isUpdate(config.getEvent()) || isInsert(config.getEvent())){
            // ???????????????????????????????????????
            final String queryCount = config.getCommand().get(ConnectorConstant.OPERTION_QUERY_COUNT_EXIST);
            final String event = existRow(connectorMapper, queryCount, row.get(pkField.getName())) ? ConnectorConstant.OPERTION_UPDATE : ConnectorConstant.OPERTION_INSERT;
            logger.warn("{}?????????{}??????, ????????????{}, {}", config.getTableName(), config.getEvent(), event, row);
            writer(result, connectorMapper, config, pkField, row, event);
        }
    }

    private void writer(Result result, DatabaseConnectorMapper connectorMapper, WriterBatchConfig config, Field pkField, Map row,
                        String event) {
        // 1????????? SQL
        String sql = config.getCommand().get(event);

        List<Field> fields = new ArrayList<>(config.getFields());
        // Update / Delete
        if (!isInsert(event)) {
            if (isDelete(event)) {
                fields.clear();
            } else if (isUpdate(event)) {
                fields.remove(pkField);
            }
            fields.add(pkField);
        }

        try {
            // 2???????????????
            int execute = connectorMapper.execute(databaseTemplate -> databaseTemplate.update(sql, batchRow(fields, row)));
            if (execute == 0) {
                throw new ConnectorException(String.format("????????????[%s]??????", event));
            }
            result.getSuccessData().add(row);
        } catch (Exception e) {
            result.getFailData().add(row);
            result.getError().append("SQL:").append(sql).append(System.lineSeparator())
                    .append("DATA:").append(row).append(System.lineSeparator())
                    .append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
            logger.error("??????{}??????: {}, DATA:{}", event, e.getMessage(), row);
        }
    }

    private boolean existRow(DatabaseConnectorMapper connectorMapper, String sql, Object value) {
        int rowNum = 0;
        try {
            rowNum = connectorMapper.execute(databaseTemplate -> databaseTemplate.queryForObject(sql, new Object[]{value}, Integer.class));
        } catch (Exception e) {
            logger.error("???????????????????????????:{}???SQL:{},??????:{}", e.getMessage(), sql, value);
        }
        return rowNum > 0;
    }

    private boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        return !CollectionUtils.isEmpty(pk) && pk.contains(name);
    }

}