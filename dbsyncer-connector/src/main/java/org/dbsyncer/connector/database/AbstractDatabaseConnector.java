package org.dbsyncer.connector.database;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.enums.SqlBuilderEnum;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.connector.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractDatabaseConnector implements Database {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 获取表元信息SQL, 具体实现交给对应的连接器
     *
     * @param config
     * @param tableName
     * @return
     */
    protected abstract String getMetaSql(DatabaseConfig config, String tableName);

    @Override
    public boolean isAlive(ConnectorConfig config) {
        DatabaseConfig cfg = (DatabaseConfig) config;
        Connection connection = null;
        try {
            connection = JDBCUtil.getConnection(cfg.getDriverClassName(), cfg.getUrl(), cfg.getUsername(), cfg.getPassword());
        } catch (Exception e) {
            logger.error("Failed to connect:{}", cfg.getUrl(), e.getMessage());
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
            String sql = "show tables";
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
            String metaSql = getMetaSql(cfg, tableName);
            metaInfo = DatabaseUtil.getMetaInfo(jdbcTemplate, metaSql);
        } catch (Exception e) {
            logger.error("getMetaInfo failed", e);
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return metaInfo;
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

    @Override
    public String getQueryFilterSql(List<Filter> filter) {
        if (CollectionUtils.isEmpty(filter)) {
            return "";
        }
        // 过滤条件SQL
        StringBuilder condition = new StringBuilder();

        // 拼接并且SQL
        String addSql = getFilterSql(OperationEnum.AND.getName(), filter);
        // 如果Add条件存在
        if (StringUtils.isNotBlank(addSql)) {
            condition.append(addSql);
        }

        // 拼接或者SQL
        String orSql = getFilterSql(OperationEnum.OR.getName(), filter);
        // 如果Or条件和Add条件都存在
        if (StringUtils.isNotBlank(orSql) && StringUtils.isNotBlank(addSql)) {
            condition.append(" OR ").append(orSql);
        }

        // 如果有条件加上 WHERE
        StringBuilder queryFilterSql = new StringBuilder();
        if (StringUtils.isNotBlank(condition.toString())) {
            // WHERE (USER.USERNAME = 'zhangsan' AND USER.AGE='20') OR (USER.TEL='18299996666')
            queryFilterSql.insert(0, " WHERE ").append(condition);
        }
        return queryFilterSql.toString();
    }

    @Override
    public String getJdbcSql(String opertion, DatabaseConfig config, Table table, String queryFilterSQL) {
        if(null == table){
            logger.error("Table can not be null.");
            throw new ConnectorException("Table can not be null.");
        }
        List<Field> column = table.getColumn();
        if (CollectionUtils.isEmpty(column)) {
            logger.error("Table column can not be empty.");
            throw new ConnectorException("Table column can not be empty.");
        }
        // 获取主键
        String pk = null;
        // 去掉重复的查询字段
        List<String> filedNames = new ArrayList<String>();
        for (Field c : column) {
            if(c.isPk()){
                pk = c.getName();
            }
            String name = c.getName();
            // 如果没有重复
            if (StringUtils.isNotBlank(name) && !filedNames.contains(name)) {
                filedNames.add(name);
            }
        }
        if(StringUtils.isBlank(pk)){
            logger.error("Table primary key can not be empty.");
            throw new ConnectorException("Table primary key can not be empty.");
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
        return SqlBuilderEnum.getSqlBuilder(opertion).buildSql(config, tableName, pk, filedNames, queryFilterSQL, this);
    }

    @Override
    public String getJdbcSqlQuartzRange(String tableName, String quartzFiled, String queryFilterSQL) {
        quartzFiled = tableName + "." + quartzFiled;
        StringBuilder f = new StringBuilder();
        // 如果没有加过滤条件就拼接WHERE语法
        // TB_USER.LASTDATE > ? AND TB_USER.LASTDATE <= ?
        f.append(StringUtils.isBlank(queryFilterSQL) ? " WHERE " : " AND ");
        // LASTDATE > '2017-11-10 11:07:41' AND LASTDATE <= '2017-11-10 11:30:01' ORDER BY LASTDATE
        f.append(quartzFiled).append(" > ?").append(" AND ").append(quartzFiled).append(" <= ?").append(" ORDER BY ").append(quartzFiled);
        return f.toString();
    }

    @Override
    public String getJdbcSqlQuartzAll(String tableName, String quartzFiled, String queryFilterSQL) {
        StringBuilder f = new StringBuilder();
        // 如果没有加过滤条件就拼接WHERE语法
        f.append(StringUtils.isBlank(queryFilterSQL) ? " WHERE " : " AND ");
        // TB_USER.LASTDATE <= ?
        f.append(tableName).append(".").append(quartzFiled).append(" <= ?").append(" ORDER BY ").append(tableName).append(".").append(quartzFiled);
        // TB_USER.LASTDATE <= '2017-11-10 11:07:41'
        return f.toString();
    }

    @Override
    public String getJdbcSqlQuartzMax(String tableName, String quartzFiled) {
        StringBuilder f = new StringBuilder();
        // SELECT MAX(USER.LASTDATE) FROM TB_USER
        f.append("SELECT MAX(").append(tableName).append(".").append(quartzFiled).append(") FROM ").append(tableName);
        return f.toString();
    }

    @Override
    public void batchRowsSetter(PreparedStatement ps, List<Field> fields, Map<String, Object> row) {
        if (CollectionUtils.isEmpty(fields)) {
            logger.error("Rows fields can not be empty.");
            throw new ConnectorException(String.format("Rows fields can not be empty."));
        }
        int fieldSize = fields.size();
        Field f = null;
        int type;
        Object val = null;
        for (int i = 0; i < fieldSize; i++) {
            // 取出字段和对应值
            f = fields.get(i);
            type = f.getType();
            val = row.get(f.getName());
            DatabaseUtil.preparedStatementSetter(ps, i + 1, type, val);
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
        return Arrays.asList(cfg.getSql());
    }

    /**
     * 获取DQl元信息
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
            metaInfo = DatabaseUtil.getMetaInfo(jdbcTemplate, cfg.getSql());
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            // 释放连接
            this.close(jdbcTemplate);
        }
        return metaInfo;
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
        if(CollectionUtils.isEmpty(list)){
            return "";
        }

        int size = list.size();
        int end = size - 1;
        StringBuilder sql = new StringBuilder();
        sql.append("(");
        Filter c = null;
        for (int i = 0; i < size; i++) {
            c = list.get(i);
            // USER = 'zhangsan'
            sql.append(c.getName()).append(c.getFilter()).append("'").append(c.getValue()).append("'");
            if (i < end) {
                sql.append(" ").append(queryOperator).append(" ");
            }
        }
        sql.append(")");
        return sql.toString();
    }

}