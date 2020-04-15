package org.dbsyncer.connector.util;

import com.sun.rowset.CachedRowSetImpl;
import org.apache.commons.dbcp.BasicDataSource;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DatabaseUtil {

    private DatabaseUtil() {
    }

    public static JdbcTemplate getJdbcTemplate(DatabaseConfig config) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(config.getDriverClassName());
        dataSource.setUrl(config.getUrl());
        dataSource.setUsername(config.getUsername());
        dataSource.setPassword(config.getPassword());
        // 是否自动回收超时连接
        dataSource.setRemoveAbandoned(true);
        // 超时时间(以秒数为单位)
        dataSource.setRemoveAbandonedTimeout(60);
        return new JdbcTemplate(dataSource);
    }

    public static void close(JdbcTemplate jdbcTemplate) throws SQLException {
        if (null != jdbcTemplate) {
            DataSource dataSource = jdbcTemplate.getDataSource();
            BasicDataSource ds = (BasicDataSource) dataSource;
            ds.close();
        }
    }

    /**
     * 获取数据库表元数据信息
     *
     * @param jdbcTemplate
     * @param metaSql      查询元数据
     * @return
     */
    public static MetaInfo getMetaInfo(JdbcTemplate jdbcTemplate, String metaSql) throws SQLException {
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(metaSql);
        ResultSetWrappingSqlRowSet rowSet = (ResultSetWrappingSqlRowSet) sqlRowSet;
        CachedRowSetImpl resultSet = (CachedRowSetImpl) rowSet.getResultSet();
        SqlRowSetMetaData metaData = rowSet.getMetaData();

        // 查询表字段信息
        int columnCount = metaData.getColumnCount();
        if (1 > columnCount) {
            throw new ConnectorException("查询表字段不能为空.");
        }
        Connection connection = jdbcTemplate.getDataSource().getConnection();
        DatabaseMetaData md = connection.getMetaData();

        List<Field> fields = new ArrayList<>(columnCount);
        // <表名,[主键, ...]>
        Map<String, List<String>> tables = new HashMap<>();
        String name = null;
        String label = null;
        String typeName = null;
        String tableName = null;
        int columnType;
        boolean pk;
        for (int i = 1; i <= columnCount; i++) {
            tableName = metaData.getTableName(i);
            if (null == tables.get(tableName)) {
                tables.putIfAbsent(tableName, findTablePrimaryKeys(md, tableName));
            }
            name = metaData.getColumnName(i);
            label = metaData.getColumnLabel(i);
            typeName = metaData.getColumnTypeName(i);
            columnType = metaData.getColumnType(i);
            pk = isPk(tables, tableName, name);
            fields.add(new Field(label, typeName, columnType, pk));
        }
        return new MetaInfo(fields, resultSet.size());
    }

    private static boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        return !CollectionUtils.isEmpty(pk) && pk.contains(name);
    }

    private static List<String> findTablePrimaryKeys(DatabaseMetaData md, String tableName) throws SQLException {
        //根据表名获得主键结果集
        ResultSet rs = md.getPrimaryKeys(null, null, tableName);
        List<String> primaryKeys = new ArrayList<>();
        while (rs.next()) {
            primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
        return primaryKeys;
    }

}