package org.dbsyncer.connector.util;

import org.apache.commons.dbcp.DelegatingDatabaseMetaData;
import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.database.DatabaseTemplate;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.util.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DatabaseUtil {

    private DatabaseUtil() {
    }

    public static Connection getConnection(DatabaseConfig config)
            throws SQLException, ClassNotFoundException {
        if (null != config.getDriverClassName()) {
            Class.forName(config.getDriverClassName());
        }
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }

    public static void close(AutoCloseable rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取数据库表元数据信息
     *
     * @param databaseTemplate
     * @param metaSql          查询元数据
     * @param tableName        表名
     * @return
     */
    public static MetaInfo getMetaInfo(DatabaseTemplate databaseTemplate, String metaSql, String tableName) throws SQLException {
        SqlRowSet sqlRowSet = databaseTemplate.queryForRowSet(metaSql);
        ResultSetWrappingSqlRowSet rowSet = (ResultSetWrappingSqlRowSet) sqlRowSet;
        SqlRowSetMetaData metaData = rowSet.getMetaData();

        // 查询表字段信息
        int columnCount = metaData.getColumnCount();
        if (1 > columnCount) {
            throw new ConnectorException("查询表字段不能为空.");
        }
        List<Field> fields = new ArrayList<>(columnCount);
        // <表名,[主键, ...]>
        Map<String, List<String>> tables = new HashMap<>();
        try {
            DatabaseMetaData md = databaseTemplate.getConnection().getMetaData();
            String name = null;
            String label = null;
            String typeName = null;
            String table = null;
            int columnType;
            boolean pk;
            for (int i = 1; i <= columnCount; i++) {
                table = StringUtils.isNotBlank(tableName) ? tableName : metaData.getTableName(i);
                if (null == tables.get(table)) {
                    tables.putIfAbsent(table, findTablePrimaryKeys(md, table));
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
     * 获取数据库名称
     *
     * @param conn
     * @return
     * @throws NoSuchFieldException
     * @throws SQLException
     * @throws IllegalAccessException
     */
    public static String getDataBaseName(Connection conn) throws NoSuchFieldException, SQLException, IllegalAccessException {
        DelegatingDatabaseMetaData md = (DelegatingDatabaseMetaData) conn.getMetaData();
        DatabaseMetaData delegate = md.getDelegate();
        String driverVersion = delegate.getDriverVersion();
        boolean driverThanMysql8 = isDriverVersionMoreThanMysql8(driverVersion);
        String databaseProductVersion = delegate.getDatabaseProductVersion();
        boolean dbThanMysql8 = isDatabaseProductVersionMoreThanMysql8(databaseProductVersion);
        Assert.isTrue(driverThanMysql8 == dbThanMysql8, String.format("当前驱动%s和数据库%s版本不一致.", driverVersion, databaseProductVersion));

        Class clazz = delegate.getClass().getSuperclass();
        java.lang.reflect.Field field = clazz.getDeclaredField("database");
        field.setAccessible(true);
        return (String) field.get(delegate);
    }

    /**
     * 返回主键名称
     *
     * @param table
     * @param quotation
     * @return
     */
    public static String findTablePrimaryKey(Table table, String quotation) {
        if (null != table) {
            List<Field> column = table.getColumn();
            if (!CollectionUtils.isEmpty(column)) {
                for (Field c : column) {
                    if (c.isPk()) {
                        return new StringBuilder(quotation).append(c.getName()).append(quotation).toString();
                    }
                }
            }
        }
        throw new ConnectorException("Table primary key can not be empty.");
    }

    /**
     * Mysql 8.0
     * <p>mysql-connector-java-8.0.11</p>
     * <p>mysql-connector-java-5.1.40</p>
     *
     * @param driverVersion
     * @return
     */
    private static boolean isDriverVersionMoreThanMysql8(String driverVersion) {
        return StringUtils.startsWith(driverVersion, "mysql-connector-java-8");
    }

    /**
     * Mysql 8.0
     * <p>8.0.0-log</p>
     * <p>5.7.26-log</p>
     *
     * @param databaseProductVersion
     * @return
     */
    private static boolean isDatabaseProductVersionMoreThanMysql8(String databaseProductVersion) {
        return StringUtils.startsWith(databaseProductVersion, "8");
    }

    private static boolean isPk(Map<String, List<String>> tables, String tableName, String name) {
        List<String> pk = tables.get(tableName);
        return !CollectionUtils.isEmpty(pk) && pk.contains(name);
    }

    private static List<String> findTablePrimaryKeys(DatabaseMetaData md, String tableName) throws SQLException {
        //根据表名获得主键结果集
        ResultSet rs = null;
        List<String> primaryKeys = new ArrayList<>();
        try {
            rs = md.getPrimaryKeys(null, null, tableName);
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            close(rs);
        }
        return primaryKeys;
    }

}