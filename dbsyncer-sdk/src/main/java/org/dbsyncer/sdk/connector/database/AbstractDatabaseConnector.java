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
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
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
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.CustomData;
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

    public abstract String buildJdbcUrl(DatabaseConfig connectorConfig, String database);

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SQL;
    }

    @Override
    public Class<DatabaseConfig> getConfigClass() {
        return DatabaseConfig.class;
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        return new DatabaseConnectorInstance(config, context.getCatalog(), context.getSchema());
    }

    @Override
    public void disconnect(DatabaseConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(DatabaseConnectorInstance connectorInstance) {
        Integer count = connectorInstance.execute(databaseTemplate->databaseTemplate.queryForObject(getValidationQuery(), Integer.class));
        return null != count && count > 0;
    }

    @Override
    public List<Table> getTable(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return connectorInstance.execute(databaseTemplate-> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            // 兼容处理 schema 和 catalog
            String effectiveCatalog = getCatalog(context.getCatalog(), conn);
            String effectiveSchema = getSchema(context.getSchema(), conn);

            String[] types = {TableTypeEnum.TABLE.getCode(), TableTypeEnum.VIEW.getCode(), TableTypeEnum.MATERIALIZED_VIEW.getCode()};
            final ResultSet rs = conn.getMetaData().getTables(effectiveCatalog, effectiveSchema, null, types);
            logger.info("Using connector type: {}, catalog: {}, schema: {}", getConnectorType(), effectiveCatalog, effectiveSchema);
            List<Table> tables = new ArrayList<>();
            while (rs.next()) {
                final String tableName = rs.getString("TABLE_NAME");
                final String tableType = rs.getString("TABLE_TYPE");
                Table table = new Table();
                table.setName(tableName);
                table.setType(tableType);
                tables.add(table);
            }
            return tables;
        });
    }

    @Override
    public List<MetaInfo> getMetaInfo(DatabaseConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return connectorInstance.execute(databaseTemplate-> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            final String catalog = getCatalog(context.getCatalog(), conn);
            final String schema = getSchema(context.getSchema(), conn);
            DatabaseMetaData metaData = conn.getMetaData();
            List<MetaInfo> metaInfos = new ArrayList<>();
            for (Table table : context.getTablePatterns()) {
                // 自定义SQL
                if (TableTypeEnum.getTableType(table.getType()) == getExtendedTableType()) {
                    getMetaInfoWithSQL(databaseTemplate, metaInfos, catalog, schema, table);
                    continue;
                }

                String tableName = table.getName();
                List<Field> fields = new ArrayList<>();
                List<String> primaryKeys = findTablePrimaryKeys(metaData, catalog, schema, tableName);
                try (ResultSet columnMetadata = metaData.getColumns(catalog, schema, tableName, null)) {
                    while (columnMetadata.next()) {
                        String columnName = columnMetadata.getString("COLUMN_NAME");
                        int columnType = columnMetadata.getInt("DATA_TYPE");
                        String typeName = columnMetadata.getString("TYPE_NAME");
                        int columnSize = Math.max(0, columnMetadata.getInt("COLUMN_SIZE"));
                        int ratio = Math.max(0, columnMetadata.getInt("DECIMAL_DIGITS"));
                        fields.add(new Field(columnName, typeName, columnType, primaryKeys.contains(columnName), columnSize, ratio));
                    }
                }
                MetaInfo metaInfo = new MetaInfo();
                metaInfo.setTable(tableName);
                metaInfo.setTableType(TableTypeEnum.TABLE.getCode());
                metaInfo.setColumn(fields);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        });
    }

    private void getMetaInfoWithSQL(DatabaseTemplate databaseTemplate, List<MetaInfo> metaInfos, String catalog, String schema, Table t) throws SQLException {
        Object val = t.getExtInfo().get(ConnectorConstant.CUSTOM_TABLE_SQL);
        if (val == null) {
            return;
        }
        String originalSql = String.valueOf(val);
        String sql = originalSql.toUpperCase();
        sql = sql.replace("\t", " ");
        sql = sql.replace("\r", " ");
        sql = sql.replace("\n", " ");
        String metaSql = StringUtil.contains(sql, " WHERE ") ? originalSql + " AND 1!=1 " : originalSql + " WHERE 1!=1 ";
        String tableName = t.getName();

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
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            DatabaseMetaData md = connection.getMetaData();
            Connection conn = connection.getConnection();
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
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setTable(tableName);
        metaInfo.setTableType(getExtendedTableType().getCode());
        metaInfo.setColumn(fields);
        metaInfos.add(metaInfo);
    }

    private boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        if (CollectionUtils.isEmpty(pk)) {
            return false;
        }
        return pk.stream().anyMatch(key->key.equalsIgnoreCase(name));
    }

    @Override
    public long getCount(DatabaseConnectorInstance connectorInstance, MetaContext metaContext) {
        Map<String, String> command = metaContext.getCommand();
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
            return connectorInstance.execute(databaseTemplate-> {
                Long count = databaseTemplate.queryForObject(queryCountSql, Long.class);
                return count == null ? 0 : count;
            });
        } catch (EmptyResultDataAccessException e) {
            return 0;
        }
    }

    @Override
    public Result reader(DatabaseConnectorInstance connectorInstance, ReaderContext context) {
        // 1、获取查询SQL
        boolean supportedCursor = context.isSupportedCursor() && context.getCursors() != null && context.getCursors().length > 0;
        String queryKey = supportedCursor ? ConnectorConstant.OPERTION_QUERY_CURSOR : ConnectorConstant.OPERTION_QUERY;
        final String querySql = context.getCommand().get(queryKey);
        Assert.hasText(querySql, "查询语句不能为空.");

        // 2、设置参数
        Collections.addAll(context.getArgs(), supportedCursor ? getPageCursorArgs(context) : getPageArgs(context));

        // 3、执行SQL
        List<Map<String, Object>> list = connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(querySql, context.getArgs().toArray()));

        // 4、返回结果集
        return new Result(list);
    }

    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        String event = context.getEvent();
        List<Map> data = context.getTargetList();
        List<Field> targetFields = context.getTargetFields();

        if (CollectionUtils.isEmpty(targetFields)) {
            logger.error("writer fields can not be empty.");
            throw new SdkException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new SdkException("writer data can not be empty.");
        }
        // 1、获取SQL
        List<Field> fields = new ArrayList<>(targetFields);
        String executeSql;
        if (isDelete(event)) {
            executeSql = context.getCommand().get(event);
            fields.clear();
            fields.addAll(PrimaryKeyUtil.findExistPrimaryKeyFields(targetFields));
        } else if (context.isForceUpdate()) {
            // 开启覆盖
            executeSql = context.getCommand().get(ConnectorConstant.OPERTION_UPSERT);
        } else if (isUpdate(event)) {
            // 修改操作
            fields.addAll(PrimaryKeyUtil.findPrimaryKeyFields(fields));
            executeSql = context.getCommand().get(event);
        } else {
            // 新增操作
            executeSql = context.getCommand().get(event);
        }
        if (StringUtil.isBlank(executeSql)) {
            logger.error("事件:{}, 执行SQL不能为空", event);
            throw new SdkException("执行SQL不能为空");
        }

        Result result = new Result();
        int[] execute = null;
        try {
            // 2、设置参数
            execute = connectorInstance.execute(databaseTemplate-> {
                SimpleConnection connection = databaseTemplate.getSimpleConnection();
                try {
                    // 手动提交事务
                    connection.setAutoCommit(false);
                    int[] r = databaseTemplate.batchUpdate(executeSql, batchRows(fields, data));
                    connection.commit();
                    return r;
                } catch (Exception e) {
                    // 异常回滚
                    connection.rollback();
                    throw e;
                } finally {
                    connection.setAutoCommit(true);
                }
            });
        } catch (Exception e) {
            // 出现失败时，服务降级为逐条处理
            forceUpdate(connectorInstance, context, executeSql, fields, event, data, result);
        }

        if (null != execute) {
            int batchSize = execute.length;
            for (int i = 0; i < batchSize; i++) {
                /**
                 * MySQL返回结果：
                 * With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as a new row, 2 if an existing row is updated, and 0 if an existing row is set to its current values.
                 */
                if (execute[i] == 1 || execute[i] == 2 || execute[i] == -2) {
                    result.getSuccessData().add(data.get(i));
                    continue;
                }
                result.getFailData().add(data.get(i));
            }
        }
        return result;
    }

    private void forceUpdate(DatabaseConnectorInstance connectorInstance, PluginContext context, String executeSql, List<Field> fields, String event, List<Map> data, Result result) {
        data.forEach(row-> {
            try {
                int execute = connectorInstance.execute(databaseTemplate->databaseTemplate.update(executeSql, batchRow(fields, row)));
                if (execute == 0) {
                    throw new SdkException("数据不存在或执行异常");
                }
                result.getSuccessData().add(row);
                printTraceLog(context, event, row, Boolean.TRUE, null);
            } catch (Exception e) {
                result.getFailData().add(row);
                result.getError().append(context.getTraceId()).append(" SQL:").append(executeSql).append(System.lineSeparator()).append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
                printTraceLog(context, event, row, Boolean.FALSE, e.getMessage());
            }
        });
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        TableTypeEnum tableType = TableTypeEnum.getTableType(table.getType());
        if (tableType == getExtendedTableType()) {
            return getSourceCommandWithSQL(commandConfig);
        }

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
        String schema = buildSchemaWithQuotation(commandConfig.getSchema());
        // 同步字段
        List<Field> columns = filterColumn(table.getColumn());
        // 获取过滤SQL
        final String queryFilterSql = getQueryFilterSql(commandConfig);

        // 获取分页SQL
        Map<String, String> map = new HashMap<>();
        SqlBuilderConfig buildSqlConfig = new SqlBuilderConfig(this, schema, tableName, primaryKeys, columns, queryFilterSql);
        buildSql(map, SqlBuilderEnum.QUERY, buildSqlConfig);

        // 构建游标分页SQL
        buildSql(map, SqlBuilderEnum.QUERY_CURSOR, buildSqlConfig);
        // 记录实际参与游标的主键列表，用于全量同步时获取游标占位符
        map.put(ConnectorConstant.CURSOR_PK_NAMES, StringUtil.join(primaryKeys, StringUtil.COMMA));

        // 获取查询总数SQL
        map.put(SqlBuilderEnum.QUERY_COUNT.getName(), getQueryCountSql(buildSqlConfig));
        return map;
    }

    private Map<String, String> getSourceCommandWithSQL(CommandConfig commandConfig) {
        // 获取过滤SQL
        String queryFilterSql = getQueryFilterSql(commandConfig);
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();
        List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(table);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return map;
        }

        // 获取查询SQL
        String querySql = String.valueOf(table.getExtInfo().get(ConnectorConstant.CUSTOM_TABLE_SQL));

        // 存在条件
        if (StringUtil.isNotBlank(queryFilterSql)) {
            querySql += queryFilterSql;
        }
        PageSql pageSql = new PageSql(querySql, StringUtil.EMPTY, primaryKeys, table.getColumn());
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(pageSql));

        // 获取查询总数SQL
        map.put(SqlBuilderEnum.QUERY_COUNT.getName(), "SELECT COUNT(*) FROM (" + querySql + ") DBS_T");
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
        String schema = buildSchemaWithQuotation(commandConfig.getSchema());
        // 同步字段
        List<Field> column = filterColumn(table.getColumn());
        SqlBuilderConfig config = new SqlBuilderConfig(this, schema, tableName, primaryKeys, column, null);

        // 获取增删改SQL
        Map<String, String> map = new HashMap<>();
        if (commandConfig.isForceUpdate()) {
            DatabaseConnectorInstance instance = (DatabaseConnectorInstance) commandConfig.getConnectorInstance();
            map.put(ConnectorConstant.OPERTION_UPSERT, buildUpsertSql(instance, config));
        } else {
            map.put(SqlBuilderEnum.INSERT.getName(), buildInsertSql(config));
            buildSql(map, SqlBuilderEnum.UPDATE, config);
        }
        buildSql(map, SqlBuilderEnum.DELETE, config);
        return map;
    }

    /**
     * 获取数据库名
     *
     * @param database
     * @param connection
     * @return
     */
    protected String getCatalog(String database, Connection connection) throws SQLException {
        return StringUtil.isNotBlank(database) ? database : connection.getCatalog();
    }

    /**
     * 获取架构名
     *
     * @param schema
     * @param connection
     * @return
     */
    protected String getSchema(String schema, Connection connection) throws SQLException {
        return StringUtil.isNotBlank(schema) ? schema : connection.getSchema();
    }

    /**
     * 获取架构名
     */
    private String buildSchemaWithQuotation(String schema) {
        StringBuilder s = new StringBuilder();
        if (StringUtil.isNotBlank(schema)) {
            s.append(buildWithQuotation(schema)).append(".");
        }
        return s.toString();
    }

    /**
     * 构建复合键的游标分页条件（支持任意数量的主键）
     * 
     * <p>对于单字段主键 (pk1)：生成条件：pk1 > ?</p>
     * <p>对于2字段主键 (pk1, pk2)：生成条件：(pk1 > ?) OR (pk1 = ? AND pk2 > ?)</p>
     * <p>对于3字段主键 (pk1, pk2, pk3)：生成条件：(pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?)</p>
     * <p>对于n字段主键：依此类推...</p>
     * 
     * <p>原理：对于复合键 (pk1, pk2, ..., pkn)，要查询所有大于 (v1, v2, ..., vn) 的记录，
     * 需要满足：(pk1 > v1) OR (pk1 = v1 AND pk2 > v2) OR ... OR (pk1 = v1 AND pk2 = v2 AND ... AND pkn > vn)</p>
     * 
     * @param sql SQL构建器
     * @param primaryKeys 主键列表（支持任意长度）
     * @param noCondition 无过滤条件
     */
    private void buildCursorCondition(StringBuilder sql, List<String> primaryKeys, boolean noCondition) {
        if (primaryKeys.isEmpty()) {
            return;
        }

        // 单字段主键：直接使用 pk > ?
        if (primaryKeys.size() == 1) {
            if (!noCondition) {
                sql.append(" AND ");
            }
            sql.append(primaryKeys.get(0)).append(" > ? ");
            return;
        }

        // 复合键：构建 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR ...
        if (!noCondition) {
            sql.append(" AND ");
        }
        sql.append("(");

        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append("(");

            // 前面的字段都是等号
            for (int j = 0; j < i; j++) {
                if (j > 0) {
                    sql.append(" AND ");
                }
                sql.append(primaryKeys.get(j)).append(" = ? ");
            }

            // 最后一个字段使用大于号
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(primaryKeys.get(i)).append(" > ? ");
            sql.append(")");
        }

        sql.append(") ");
    }

    /**
     * 构建游标分页的WHERE条件和ORDER BY子句（不包含LIMIT/ROWNUM等分页限制）
     *
     * <p>用于在子类的getPageCursorSql方法中构建游标分页SQL的公共部分</p>
     *
     * @param sql    SQL构建器
     * @param config PageSql配置
     */
    protected void buildCursorConditionAndOrderBy(StringBuilder sql, PageSql config) {
        boolean noCondition = StringUtil.isBlank(config.getQueryFilter());
        // 没有过滤条件
        if (noCondition) {
            sql.append(" WHERE ");
        }

        // 使用buildPrimaryKeys处理主键
        List<String> primaryKeys = buildPrimaryKeys(config.getPrimaryKeys());

        // 构建复合键的游标分页条件: (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR ...
        buildCursorCondition(sql, primaryKeys, noCondition);

        // 添加 ORDER BY - 必须按主键排序以保证游标分页的稳定性
        sql.append(" ORDER BY ").append(StringUtil.join(primaryKeys, StringUtil.COMMA));
    }

    /**
     * 仅构建游标分页的WHERE条件（不包含ORDER BY）
     *
     * <p>用于SQL Server等需要外层排序的场景，避免内外层重复ORDER BY</p>
     *
     * @param sql    SQL构建器
     * @param config PageSql配置
    w     */
    protected void buildCursorConditionOnly(StringBuilder sql, PageSql config) {
        boolean noCondition = StringUtil.isBlank(config.getQueryFilter());
        // 没有过滤条件
        if (noCondition) {
            sql.append(" WHERE ");
        }

        // 使用buildPrimaryKeys处理主键
        List<String> primaryKeys = buildPrimaryKeys(config.getPrimaryKeys());

        // 构建复合键的游标分页条件: (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR ...
        buildCursorCondition(sql, primaryKeys, noCondition);
    }

    /**
     * 为分页查询添加ORDER BY子句（按主键排序）
     *
     * <p>用于在getPageSql方法中为非游标分页查询添加ORDER BY子句，确保分页结果的一致性</p>
     * <p>注意：非游标分页场景也必须添加ORDER BY，否则会导致数据重复或遗漏</p>
     *
     * @param sql SQL构建器
     * @param config PageSql配置
     */
    protected void appendOrderByPrimaryKeys(StringBuilder sql, PageSql config) {
        // 使用buildPrimaryKeys处理主键
        final List<String> primaryKeys = buildPrimaryKeys(config.getPrimaryKeys());
        // 添加 ORDER BY - 必须按主键排序以保证游标分页的稳定性
        sql.append(" ORDER BY ").append(StringUtil.join(primaryKeys, StringUtil.COMMA));
    }

    /**
     * 构建游标分页的参数数组核心逻辑（不包括数据库特定的OFFSET等参数）
     * 
     * <p>返回游标条件参数，不包含pageSize等分页参数</p>
     * 
     * @param cursors 当前游标值数组
     * @return 游标条件参数数组，如果无游标则返回null
     */
    protected Object[] buildCursorArgs(Object[] cursors) {
        if (null == cursors || cursors.length == 0) {
            return null;
        }
        // 单字段主键：只需要 pk > ?，参数为 [last_pk]
        int pkCount = cursors.length;
        if (pkCount == 1) {
            return new Object[]{cursors[0]};
        }

        // 复合键（支持任意数量的主键）
        // WHERE 条件为 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?) OR ...
        //
        // 参数数量计算：等差数列求和
        // - 1个主键：1个参数 (pk1 > ?)
        // - 2个主键：1+2=3个参数 (pk1 > ?) OR (pk1 = ? AND pk2 > ?)
        // - 3个主键：1+2+3=6个参数 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?)
        // - n个主键：1+2+...+n = n*(n+1)/2 个参数
        //
        // 参数顺序示例（2个主键，cursors=[v1, v2]）：
        // [v1, v1, v2] 对应条件 (pk1 > v1) OR (pk1 = v1 AND pk2 > v2)
        // 参数顺序示例（3个主键，cursors=[v1, v2, v3]）：
        // [v1, v1, v2, v1, v2, v3] 对应条件 (pk1 > v1) OR (pk1 = v1 AND pk2 > v2) OR (pk1 = v1 AND pk2 = v2 AND pk3 > v3)
        int paramCount = pkCount * (pkCount + 1) / 2;
        Object[] cursorArgs = new Object[paramCount];

        int index = 0;
        // 遍历每个主键字段，生成对应的参数
        for (int i = 0; i < pkCount; i++) {
            // 对于第 i 个主键，前面的所有主键都需要等号条件
            for (int j = 0; j < i; j++) {
                cursorArgs[index++] = cursors[j];
            }
            // 第 i 个主键使用大于号条件
            cursorArgs[index++] = cursors[i];
        }

        return cursorArgs;
    }

    /**
     * 获取查询条件SQL
     *
     * @param commandConfig
     * @return
     */
    private String getQueryFilterSql(CommandConfig commandConfig) {
        List<Filter> filter = commandConfig.getFilter();
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        Table table = commandConfig.getTable();
        Map<String, Field> fieldMap = new HashMap<>();
        table.getColumn().forEach(field->fieldMap.put(field.getName(), field));

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
        Optional<Filter> sqlFilter = filter.stream().filter(f->StringUtil.equals(f.getOperation(), OperationEnum.SQL.getName())).findFirst();
        sqlFilter.ifPresent(f->sql.append(f.getValue()));

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
     * 根据过滤条件获取查询SQL
     *
     * @param fieldMap
     * @param operator {@link OperationEnum}
     * @param filter
     * @return
     */
    private String buildFilterSql(Map<String, Field> fieldMap, String operator, List<Filter> filter) {
        List<Filter> list = filter.stream().filter(f->StringUtil.equals(f.getOperation(), operator)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        int size = list.size();
        int end = size - 1;
        StringBuilder sql = new StringBuilder();
        sql.append("(");
        Filter c = null;
        for (int i = 0; i < size; i++) {
            c = list.get(i);
            Field field = fieldMap.get(c.getName());
            Assert.notNull(field, "条件字段无效.");
            // "USER" = 'zhangsan'
            sql.append(buildWithQuotation(field.getName()));
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
        // 根据表名获得主键结果集
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

    @Override
    public Result writerDDL(DatabaseConnectorInstance connectorInstance, DDLConfig config) {
        Result result = new Result();
        try {
            Assert.hasText(config.getSql(), "执行SQL语句不能为空.");
            connectorInstance.execute(databaseTemplate-> {
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

    private List<Object[]> batchRows(List<Field> fields, List<Map> data) {
        return data.stream().map(row->batchRow(fields, row)).collect(Collectors.toList());
    }

    private Object[] batchRow(List<Field> fields, Map row) {
        List<Object> args = new ArrayList<>();
        for (Field f : fields) {
            Object val = row.get(f.getName());
            if (val instanceof CustomData) {
                CustomData cd = (CustomData) val;
                args.addAll(cd.apply());
                continue;
            }
            args.add(val);
        }
        return args.toArray();
    }

    private void printTraceLog(PluginContext context, String event, Map row, boolean success, String message) {
        if (success) {
            // 仅开启traceId时才输出日志
            if (context.isEnablePrintTraceInfo()) {
                logger.info("{} {}表事件{}, 执行{}成功, {}", context.getTraceId(), context.getTargetTable().getName(), context.getEvent(), event, row);
            }
            return;
        }
        logger.error("{} {}表事件{}, 执行{}失败:{}, DATA:{}", context.getTraceId(), context.getTargetTable().getName(), context.getEvent(), event, message, row);
    }
}