package org.dbsyncer.connector.database;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.enums.SetterEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractDatabaseConnector implements Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract String getTablesSql(DatabaseConfig config);

    @Override
    public ConnectorMapper connect(ConnectorConfig config) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        try {
            return new ConnectorMapper(config, DatabaseUtil.getConnection(cfg));
        } catch (Exception e) {
            logger.error("Failed to connect:{}, message:{}", cfg.getUrl(), e.getMessage());
        }
        throw new ConnectorException(String.format("Failed to connect:%s", cfg.getUrl()));
    }

    @Override
    public void disconnect(ConnectorMapper connectorMapper) {
        try {
            DatabaseUtil.close((JdbcTemplate) connectorMapper.getConnection());
        } catch (SQLException e) {
            logger.error("Close jdbcTemplate failed: {}", e.getMessage());
        }
    }

    @Override
    public boolean isAlive(ConnectorMapper connectorMapper) {
        Integer count = (Integer) connectorMapper.execute((conn)  -> {
            PreparedStatement ps = null;
            ResultSet rs = null;
            int c;
            try {
                ps = conn.prepareStatement(getValidationQuery());
                rs = ps.executeQuery();
                rs.next();
                c = rs.getInt(1);
            } finally {
                close(rs);
                close(ps);
            }
            return c;
        });
        return null != count && count > 0;
    }

    @Override
    public String getConnectorMapperCacheKey(ConnectorConfig config) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        return String.format("%s-%s", cfg.getUrl(), cfg.getUsername());
    }

    @Override
    public List<String> getTable(ConnectorMapper config) {
        try {
            JdbcTemplate jdbcTemplate = (JdbcTemplate) config.getConnection();
            String sql = getTablesSql((DatabaseConfig) config.getConfig());
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorMapper config, String tableName) {
        try {
            JdbcTemplate jdbcTemplate = (JdbcTemplate) config.getConnection();
            String quotation = buildSqlWithQuotation();
            StringBuilder queryMetaSql = new StringBuilder("SELECT * FROM ").append(quotation).append(tableName).append(quotation).append(" WHERE 1 != 1");
            return DatabaseUtil.getMetaInfo(jdbcTemplate, queryMetaSql.toString(), tableName);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e.getCause());
        }
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
        map.put(query, buildSql(query, table, commandConfig.getOriginalTable(), queryFilterSql));

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

        // 获取查询数据行是否存在
        String quotation = buildSqlWithQuotation();
        String pk = DatabaseUtil.findTablePrimaryKey(commandConfig.getOriginalTable(), quotation);
        StringBuilder queryCount = new StringBuilder().append("SELECT COUNT(1) FROM ").append(quotation).append(table.getName()).append(
                quotation).append(" WHERE ").append(pk).append(" = ?");
        String queryCountExist = ConnectorConstant.OPERTION_QUERY_COUNT_EXIST;
        map.put(queryCountExist, queryCount.toString());
        return map;
    }

    @Override
    public long getCount(ConnectorMapper config, Map<String, String> command) {
        // 1、获取select SQL
        String queryCountSql = command.get(ConnectorConstant.OPERTION_QUERY_COUNT);
        Assert.hasText(queryCountSql, "查询总数语句不能为空.");

        try {
            // 2、获取连接
            JdbcTemplate jdbcTemplate = (JdbcTemplate) config.getConnection();

            // 3、返回结果集
            return jdbcTemplate.queryForObject(queryCountSql, Long.class);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public Result reader(ReaderConfig config) {
        // 1、获取select SQL
        String querySql = config.getCommand().get(SqlBuilderEnum.QUERY.getName());
        Assert.hasText(querySql, "查询语句不能为空.");

        try {
            // 2、获取连接
            ConnectorMapper connectorMapper = config.getConnectorMapper();
            JdbcTemplate jdbcTemplate = (JdbcTemplate) connectorMapper.getConnection();

            // 3、设置参数
            Collections.addAll(config.getArgs(), getPageArgs(config.getPageIndex(), config.getPageSize()));

            // 4、执行SQL
            List<Map<String, Object>> list = jdbcTemplate.queryForList(querySql, config.getArgs().toArray());

            // 5、返回结果集
            return new Result(new ArrayList<>(list));
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
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

        Result result = new Result();
        try {
            // 2、获取连接
            ConnectorMapper connectorMapper = config.getConnectorMapper();
            JdbcTemplate jdbcTemplate = (JdbcTemplate) connectorMapper.getConnection();

            // 3、设置参数
            final JdbcTemplate template = jdbcTemplate;
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) {
                    batchRowsSetter(template, preparedStatement, fields, fSize, data.get(i));
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

        Field pkField = null;
        // Update / Delete
        if (isUpdate(event) || isDelete(event)) {
            pkField = getPrimaryKeyField(fields);
            if (isDelete(event)) {
                fields.clear();
            }
            fields.add(pkField);
        }

        int size = fields.size();
        Result result = new Result();
        // 2、获取连接
        ConnectorMapper connectorMapper = config.getConnectorMapper();
        JdbcTemplate jdbcTemplate = (JdbcTemplate) connectorMapper.getConnection();

        int update = 0;
        try {
            // 3、设置参数
            update = jdbcTemplate.update(sql, (ps) -> {
                Field f = null;
                for (int i = 0; i < size; i++) {
                    f = fields.get(i);
                    SetterEnum.getSetter(f.getType()).set(jdbcTemplate, ps, i + 1, f.getType(), data.get(f.getName()));
                }
            });
        } catch (Exception e) {
            // 记录错误数据
            result.getFailData().add(data);
            result.getFail().set(1);
            result.getError().append("SQL:").append(sql).append(System.lineSeparator())
                    .append("DATA:").append(data).append(System.lineSeparator())
                    .append("ERROR:").append(e.getMessage()).append(System.lineSeparator());
            logger.error("SQL:{}, DATA:{}, ERROR:{}", sql, data, e.getMessage());
        }

        // 更新失败尝试插入
        if (0 == update && isUpdate(event) && null != pkField && !config.isRetry()) {
            // 插入前检查有无数据
            String queryCount = config.getCommand().get(ConnectorConstant.OPERTION_QUERY_COUNT_EXIST);
            if (!existRow(jdbcTemplate, queryCount, data.get(pkField.getName()))) {
                fields.remove(fields.size() - 1);
                config.setEvent(ConnectorConstant.OPERTION_INSERT);
                config.setRetry(true);
                logger.warn("{}表执行{}失败, 尝试执行{}", config.getTable(), event, config.getEvent());
                result = writer(config);
            }
        }
        return result;
    }

    /**
     * 获取DQL表信息
     *
     * @param config
     * @return
     */
    protected List<String> getDqlTable(ConnectorMapper config) {
        MetaInfo metaInfo = getDqlMetaInfo(config);
        Assert.notNull(metaInfo, "SQL解析异常.");
        DatabaseConfig cfg = (DatabaseConfig) config.getConfig();
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
    protected MetaInfo getDqlMetaInfo(ConnectorMapper config) {
        try {
            DatabaseConfig cfg = (DatabaseConfig) config.getConfig();
            JdbcTemplate jdbcTemplate = (JdbcTemplate) config.getConnection();
            String sql = cfg.getSql().toUpperCase();
            String queryMetaSql = StringUtils.contains(sql, " WHERE ") ? sql + " AND 1!=1 " : sql + " WHERE 1!=1 ";
            return DatabaseUtil.getMetaInfo(jdbcTemplate, queryMetaSql, cfg.getTable());
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        }
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
        String quotation = buildSqlWithQuotation();
        String pk = DatabaseUtil.findTablePrimaryKey(commandConfig.getOriginalTable(), quotation);
        map.put(SqlBuilderEnum.QUERY.getName(), getPageSql(new PageSqlConfig(querySql, pk)));

        // 获取查询总数SQL
        StringBuilder queryCount = new StringBuilder();
        queryCount.append("SELECT COUNT(1) FROM (").append(table.getName());
        if (StringUtils.isNotBlank(queryFilterSql)) {
            queryCount.append(queryFilterSql);
        }
        // Mysql
        if (appendGroupByPK) {
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
    protected String getQueryFilterSql(List<Filter> filter) {
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
     * 获取查询SQL
     *
     * @param type           {@link SqlBuilderEnum}
     * @param table
     * @param originalTable
     * @param queryFilterSQL
     * @return
     */
    protected String buildSql(String type, Table table, Table originalTable, String queryFilterSQL) {
        if (null == table) {
            logger.error("Table can not be null.");
            throw new ConnectorException("Table can not be null.");
        }
        List<Field> column = table.getColumn();
        if (CollectionUtils.isEmpty(column)) {
            logger.warn("Table column is null.");
            return null;
        }
        String pk = null;
        Set<String> mark = new HashSet<>();
        List<Field> fields = new ArrayList<>();
        for (Field c : column) {
            String name = c.getName();
            if (StringUtils.isBlank(name)) {
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
        if (StringUtils.isBlank(tableName)) {
            logger.error("Table name can not be empty.");
            throw new ConnectorException("Table name can not be empty.");
        }
        if (StringUtils.isBlank(pk)) {
            pk = DatabaseUtil.findTablePrimaryKey(originalTable, "");
        }

        SqlBuilderConfig config = new SqlBuilderConfig(this, tableName, pk, fields, queryFilterSQL, buildSqlWithQuotation());
        return SqlBuilderEnum.getSqlBuilder(type).buildSql(config);
    }

    /**
     * 健康检查
     *
     * @return
     */
    protected String getValidationQuery(){
        return "select 1";
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
     * @param jdbcTemplate 参数构造器
     * @param ps           参数构造器
     * @param fields       同步字段，例如[{name=ID, type=4}, {name=NAME, type=12}]
     * @param fSize        同步字段个数
     * @param row          同步字段对应的值，例如{ID=123, NAME=张三11}
     */
    private void batchRowsSetter(JdbcTemplate jdbcTemplate, PreparedStatement ps, List<Field> fields, int fSize, Map row) {
        Field f = null;
        int type;
        Object val = null;
        for (int i = 0; i < fSize; i++) {
            // 取出字段和对应值
            f = fields.get(i);
            type = f.getType();
            val = row.get(f.getName());
            SetterEnum.getSetter(type).set(jdbcTemplate, ps, i + 1, type, val);
        }
    }

    private boolean existRow(JdbcTemplate jdbcTemplate, String sql, Object value) {
        int rowNum = 0;
        try {
            rowNum = jdbcTemplate.queryForObject(sql, new Object[]{value}, Integer.class);
        } catch (Exception e) {
            logger.error("检查数据行存在异常:{}，SQL:{},参数:{}", e.getMessage(), sql, value);
        }
        return rowNum > 0;
    }

    private Field getPrimaryKeyField(List<Field> fields) {
        for (Field f : fields) {
            if (f.isPk()) {
                return f;
            }
        }
        throw new ConnectorException("主键为空");
    }

    private boolean isUpdate(String event) {
        return StringUtils.equals(ConnectorConstant.OPERTION_UPDATE, event);
    }

    private boolean isDelete(String event) {
        return StringUtils.equals(ConnectorConstant.OPERTION_DELETE, event);
    }

    private void close(AutoCloseable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }
}