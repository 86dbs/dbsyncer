package org.dbsyncer.connector.database;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.enums.SetterEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.connector.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractDatabaseConnector implements Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract String getTablesSql(DatabaseConfig config);

    @Override
    public boolean isAlive(ConnectorConfig config) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        Connection connection = null;
        try {
            connection = JDBCUtil.getConnection(cfg.getDriverClassName(), cfg.getUrl(), cfg.getUsername(), cfg.getPassword());
        } catch (Exception e) {
            logger.error("Failed to connect:{}, message:{}", cfg.getUrl(), e.getMessage());
        } finally {
            JDBCUtil.close(connection);
        }
        return null != connection;
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        List<String> tables = new ArrayList<>();
        DatabaseConfig databaseConfig = (DatabaseConfig) config;
        JdbcTemplate jdbcTemplate = null;
        try {
            jdbcTemplate = getJdbcTemplate(databaseConfig);
            String sql = getTablesSql(databaseConfig);
            tables = jdbcTemplate.queryForList(sql, String.class);
        } catch (Exception e) {
            logger.error("getTable failed", e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return tables;
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        JdbcTemplate jdbcTemplate = null;
        MetaInfo metaInfo = null;
        try {
            jdbcTemplate = getJdbcTemplate(cfg);
            String quotation = buildSqlWithQuotation();
            String queryMetaSql = getTableColumnSql(new StringBuilder("SELECT * FROM ").append(quotation).append(tableName).append(quotation).toString());
            metaInfo = DatabaseUtil.getMetaInfo(jdbcTemplate, queryMetaSql, tableName);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return metaInfo;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        // 获取过滤SQL
        List<Filter> filter = commandConfig.getFilter();
        String queryFilterSql = getQueryFilterSql(filter);

        // 获取查询SQL
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();

        String query = ConnectorConstant.OPERTION_QUERY;
        map.put(query, buildSql(query, table, null, queryFilterSql));

        // 获取查询总数SQL
        String quotation = buildSqlWithQuotation();
        String pk = DatabaseUtil.findTablePrimaryKey(commandConfig.getOriginalTable(), quotation);
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (SELECT 1 FROM ").append(quotation).append(table.getName()).append(quotation);
        if (StringUtils.isNotBlank(queryFilterSql)) {
            queryCount.append(queryFilterSql);
        }
        queryCount.append(" GROUP BY ").append(pk).append(") DBSYNCER_T");
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        // 获取增删改SQL
        Map<String, String> map = new HashMap<>();
        Table table = commandConfig.getTable();
        Table originalTable = commandConfig.getOriginalTable();

        String insert = SqlBuilderEnum.INSERT.getName();
        map.put(insert, buildSql(insert, table, originalTable, null));

        String update = SqlBuilderEnum.UPDATE.getName();
        map.put(update, buildSql(update, table, originalTable, null));

        String delete = SqlBuilderEnum.DELETE.getName();
        map.put(delete, buildSql(delete, table, originalTable, null));
        return map;
    }

    @Override
    public long getCount(ConnectorConfig config, Map<String, String> command) {
        // 1、获取select SQL
        String queryCountSql = command.get(ConnectorConstant.OPERTION_QUERY_COUNT);
        Assert.hasText(queryCountSql, "查询总数语句不能为空.");

        DatabaseConfig cfg = (DatabaseConfig) config;
        JdbcTemplate jdbcTemplate = null;
        try {
            // 2、获取连接
            jdbcTemplate = getJdbcTemplate(cfg);

            // 3、返回结果集
            return jdbcTemplate.queryForObject(queryCountSql, Long.class);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
    }

    @Override
    public Result reader(ReaderConfig config) {
        // 1、获取select SQL
        String querySql = config.getCommand().get(SqlBuilderEnum.QUERY.getName());
        Assert.hasText(querySql, "查询语句不能为空.");

        DatabaseConfig cfg = (DatabaseConfig) config.getConfig();
        JdbcTemplate jdbcTemplate = null;
        try {
            // 2、获取连接
            jdbcTemplate = getJdbcTemplate(cfg);

            // 3、设置参数
            Collections.addAll(config.getArgs(), getPageArgs(config.getPageIndex(), config.getPageSize()));

            // 4、执行SQL
            List<Map<String, Object>> list = jdbcTemplate.queryForList(querySql, config.getArgs().toArray());

            // 5、返回结果集
            return new Result(new ArrayList<>(list));
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
    }

    @Override
    public Result writer(WriterBatchConfig config) {
        List<Field> fields = config.getFields();
        List<Map> data = config.getData();

        // 1、获取select SQL
        String insertSql = config.getCommand().get(SqlBuilderEnum.INSERT.getName());
        Assert.hasText(insertSql, "插入语句不能为空.");
        if (CollectionUtils.isEmpty(fields)) {
            logger.error("writer fields can not be empty.");
            throw new ConnectorException("writer fields can not be empty.");
        }
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }
        final int size = data.size();
        final int fSize = fields.size();

        DatabaseConfig cfg = (DatabaseConfig) config.getConfig();
        JdbcTemplate jdbcTemplate = null;
        Result result = new Result();
        try {
            // 2、获取连接
            jdbcTemplate = getJdbcTemplate(cfg);

            // 3、设置参数
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) {
                    batchRowsSetter(preparedStatement, fields, fSize, data.get(i));
                }

                @Override
                public int getBatchSize() {
                    return size;
                }
            });

        } catch (Exception e) {
            // 记录错误数据
            result.getFailData().addAll(data);
            result.getFail().set(size);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return result;
    }

    @Override
    public Result writer(WriterSingleConfig config) {
        String event = config.getEvent();
        List<Field> fields = config.getFields();
        Map<String, Object> data = config.getData();
        // 1、获取 SQL
        String sql = config.getCommand().get(event);
        Assert.hasText(sql, "执行语句不能为空.");
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(fields)) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }

        // Update / Delete
        if (StringUtils.equals(ConnectorConstant.OPERTION_UPDATE, event)) {
            // update attrs by id
            List<Field> pkList = fields.stream().filter(f -> f.isPk()).collect(Collectors.toList());
            fields.add(pkList.get(0));
        } else if (StringUtils.equals(ConnectorConstant.OPERTION_DELETE, event)) {
            // delete by id
            List<Field> pkList = fields.stream().filter(f -> f.isPk()).collect(Collectors.toList());
            fields.clear();
            fields.add(pkList.get(0));
        }

        int size = fields.size();

        DatabaseConfig cfg = (DatabaseConfig) config.getConfig();
        JdbcTemplate jdbcTemplate = null;
        Result result = new Result();
        try {
            // 2、获取连接
            jdbcTemplate = getJdbcTemplate(cfg);

            // 3、设置参数
            int update = jdbcTemplate.update(sql, (ps) -> {
                Field f = null;
                for (int i = 0; i < size; i++) {
                    f = fields.get(i);
                    SetterEnum.getSetter(f.getType()).set(ps, i + 1, f.getType(), data.get(f.getName()));
                }
            });
            if (0 == update) {
                throw new ConnectorException(String.format("[%s]表执行%s操作失败, 数据不存在", config.getTable(), event));
            }
        } catch (Exception e) {
            // 记录错误数据
            result.getFailData().add(data);
            result.getFail().set(1);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return result;
    }

    @Override
    public JdbcTemplate getJdbcTemplate(DatabaseConfig config) {
        return DatabaseUtil.getJdbcTemplate(config);
    }

    @Override
    public void close(JdbcTemplate jdbcTemplate) {
        try {
            DatabaseUtil.close(jdbcTemplate);
        } catch (SQLException e) {
            logger.error("Close jdbcTemplate failed: {}", e.getMessage());
        }
    }

    /**
     * 获取DQL表信息
     *
     * @param config
     * @return
     */
    protected List<String> getDqlTable(ConnectorConfig config) {
        MetaInfo metaInfo = getDqlMetaInfo(config);
        Assert.notNull(metaInfo, "SQL解析异常.");
        DatabaseConfig cfg = (DatabaseConfig) config;
        List<String> tables = new ArrayList<>();
        tables.add(cfg.getSql());
        return tables;
    }

    /**
     * 获取DQL元信息
     *
     * @param config
     * @return
     */
    protected MetaInfo getDqlMetaInfo(ConnectorConfig config) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        JdbcTemplate jdbcTemplate = null;
        MetaInfo metaInfo = null;
        try {
            jdbcTemplate = getJdbcTemplate(cfg);
            String queryMetaSql = getTableColumnSql(cfg.getSql());
            metaInfo = DatabaseUtil.getMetaInfo(jdbcTemplate, queryMetaSql, cfg.getTable());
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return metaInfo;
    }

    /**
     * 获取DQL源配置
     *
     * @param commandConfig
     * @param appendGroupByPK
     * @return
     */
    protected Map<String, String> getDqlSourceCommand(CommandConfig commandConfig, boolean appendGroupByPK) {
        // 获取过滤SQL
        List<Filter> filter = commandConfig.getFilter();
        String queryFilterSql = getQueryFilterSql(filter);

        // 获取查询SQL
        Table table = commandConfig.getTable();
        Map<String, String> map = new HashMap<>();
        String querySql = table.getName();

        // 存在条件
        if (StringUtils.isNotBlank(queryFilterSql)) {
            querySql += queryFilterSql;
        }
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(null, null, querySql));

        // 获取查询总数SQL
        String quotation = buildSqlWithQuotation();
        String pk = DatabaseUtil.findTablePrimaryKey(commandConfig.getOriginalTable(), quotation);
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (").append(table.getName());
        if (StringUtils.isNotBlank(queryFilterSql)) {
            queryCount.append(queryFilterSql);
        }
        // Mysql
        if(appendGroupByPK){
            queryCount.append(" GROUP BY ").append(pk);
        }
        queryCount.append(") DBSYNCER_T");
        map.put(ConnectorConstant.OPERTION_QUERY_COUNT, queryCount.toString());
        return map;
    }

    /**
     * 查询语句表名和字段带上引号（默认不加）
     *
     * @return
     */
    protected String buildSqlWithQuotation() {
        return "";
    }

    /**
     * 获取查询条件SQL
     *
     * @param filter
     * @return
     */
    private String getQueryFilterSql(List<Filter> filter) {
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        // 过滤条件SQL
        StringBuilder sql = new StringBuilder();

        // 拼接并且SQL
        String addSql = getFilterSql(OperationEnum.AND.getName(), filter);
        // 如果Add条件存在
        if (StringUtils.isNotBlank(addSql)) {
            sql.append(addSql);
        }

        // 拼接或者SQL
        String orSql = getFilterSql(OperationEnum.OR.getName(), filter);
        // 如果Or条件和Add条件都存在
        if (StringUtils.isNotBlank(orSql) && StringUtils.isNotBlank(addSql)) {
            sql.append(" OR ");
        }
        sql.append(orSql);

        // 如果有条件加上 WHERE
        if (StringUtils.isNotBlank(sql.toString())) {
            // WHERE (USER.USERNAME = 'zhangsan' AND USER.AGE='20') OR (USER.TEL='18299996666')
            sql.insert(0, " WHERE ");
        }
        return sql.toString();
    }

    /**
     * 根据过滤条件获取查询SQL
     *
     * @param queryOperator and/or
     * @param filter
     * @return
     */
    private String getFilterSql(String queryOperator, List<Filter> filter) {
        List<Filter> list = filter.stream().filter(f -> StringUtils.equals(f.getOperation(), queryOperator)).collect(Collectors.toList());
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
            sql.append(quotation).append(c.getName()).append(quotation).append(c.getFilter()).append("'").append(c.getValue()).append("'");
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
     * @param table
     * @param originalTable
     * @param queryFilterSQL
     * @return
     */
    private String buildSql(String type, Table table, Table originalTable, String queryFilterSQL) {
        if (null == table) {
            logger.error("Table can not be null.");
            throw new ConnectorException("Table can not be null.");
        }
        List<Field> column = table.getColumn();
        if (CollectionUtils.isEmpty(column)) {
            return null;
        }
        // 获取主键
        String pk = null;
        // 去掉重复的查询字段
        List<String> filedNames = new ArrayList<>();
        for (Field c : column) {
            if (c.isPk()) {
                pk = c.getName();
            }
            String name = c.getName();
            // 如果没有重复
            if (StringUtils.isNotBlank(name) && !filedNames.contains(name)) {
                filedNames.add(name);
            }
        }
        if (CollectionUtils.isEmpty(filedNames)) {
            logger.error("The filedNames can not be empty.");
            throw new ConnectorException("The filedNames can not be empty.");
        }
        String tableName = table.getName();
        if (StringUtils.isBlank(tableName)) {
            logger.error("Table name can not be empty.");
            throw new ConnectorException("Table name can not be empty.");
        }
        if (StringUtils.isBlank(pk)) {
            pk = DatabaseUtil.findTablePrimaryKey(originalTable, "");
        }

        SqlBuilderConfig config = new SqlBuilderConfig(this, tableName, pk, filedNames, queryFilterSQL, buildSqlWithQuotation());
        return SqlBuilderEnum.getSqlBuilder(type).buildSql(config);
    }

    /**
     * @param ps     参数构造器
     * @param fields 同步字段，例如[{name=ID, type=4}, {name=NAME, type=12}]
     * @param fSize  同步字段个数
     * @param row    同步字段对应的值，例如{ID=123, NAME=张三11}
     */
    private void batchRowsSetter(PreparedStatement ps, List<Field> fields, int fSize, Map row) {
        Field f = null;
        int type;
        Object val = null;
        for (int i = 0; i < fSize; i++) {
            // 取出字段和对应值
            f = fields.get(i);
            type = f.getType();
            val = row.get(f.getName());
            SetterEnum.getSetter(type).set(ps, i + 1, type, val);
        }
    }

}