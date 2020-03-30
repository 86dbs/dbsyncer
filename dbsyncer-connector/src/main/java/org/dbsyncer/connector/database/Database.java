package org.dbsyncer.connector.database;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.connector.config.Table;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface Database extends Connector {

    JdbcTemplate getJdbcTemplate(DatabaseConfig config);

    void close(JdbcTemplate jdbcTemplate);

    String getQueryFilterSql(List<Filter> filter);

    String getPageSql(DatabaseConfig config, String tableName, String pk, String querySQL);

    String getJdbcSql(String opertion, DatabaseConfig config, Table table, String queryFilterSQL);

    String getJdbcSqlQuartzRange(String tableName, String quartzFiled, String queryFilterSQL);

    String getJdbcSqlQuartzAll(String tableName, String quartzFiled, String queryFilterSQL);

    String getJdbcSqlQuartzMax(String tableName, String quartzFiled);

    /**
     * @param ps     参数构造器
     * @param fields 同步字段，例如[{name=ID, type=4}, {name=NAME, type=12}]
     * @param row    同步字段对应的值，例如{ID=123, NAME=张三11}
     * @throws NumberFormatException
     * @throws SQLException
     */
    void batchRowsSetter(PreparedStatement ps, List<Field> fields, Map<String, Object> row) throws Exception;

}