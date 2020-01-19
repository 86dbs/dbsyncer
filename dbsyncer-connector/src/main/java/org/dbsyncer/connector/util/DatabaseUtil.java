package org.dbsyncer.connector.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

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
    public static MetaInfo getMetaInfo(JdbcTemplate jdbcTemplate, String metaSql) {
        // 查询表字段信息和总行数
        RowCountCallbackHandler handler = new RowCountCallbackHandler();
        jdbcTemplate.query(metaSql, handler);
        String[] columnNames = handler.getColumnNames();
        int[] columnTypes = handler.getColumnTypes();
        if(null == columnNames || null == columnTypes || columnNames.length != columnTypes.length){
            throw new ConnectorException("Get metaInfo column error");
        }
        int len = columnNames.length;
        List<Field> fields = new ArrayList<>(len);
        String name = null;
        String typeName = null;
        for (int i = 0; i < len; i++) {
            name = columnNames[i];
            typeName = getTypeName(columnTypes[i]);
            fields.add(new Field(name, typeName, columnTypes[i]));
        }
        return new MetaInfo(fields, handler.getRowCount());
    }

    /**
     * 根据列类型设值
     *
     * @param ps
     * @param i
     * @param type
     * @param f
     * @throws SQLException
     * @throws NumberFormatException
     */
    public static void preparedStatementSetter(PreparedStatement ps, int i, int type, Object f) {
        switch (type) {
            case Types.NUMERIC:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.NUMERIC);
                    } else {
                        ps.setInt(i, Integer.parseInt(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.NUMERIC);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.INTEGER:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.INTEGER);
                    } else {
                        ps.setInt(i, Integer.parseInt(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.INTEGER);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.BIGINT:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.BIGINT);
                    } else {
                        ps.setLong(i, Long.parseLong(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.BIGINT);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.TINYINT:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.INTEGER);
                    } else {
                        ps.setInt(i, Integer.parseInt(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.INTEGER);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.VARCHAR:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.VARCHAR);
                    } else {
                        ps.setString(i, String.valueOf(f));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.VARCHAR);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.CHAR:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.CHAR);
                    } else {
                        ps.setString(i, String.valueOf(f));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.VARCHAR);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.TIMESTAMP:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.TIMESTAMP);
                    } else {
                        ps.setTimestamp(i, Timestamp.valueOf(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.TIMESTAMP);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.DATE:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.DATE);
                    } else {
                        ps.setDate(i, java.sql.Date.valueOf(LocalDate.parse(f + "")));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.DATE);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.FLOAT:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.FLOAT);
                    } else {
                        ps.setFloat(i, Float.parseFloat(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.FLOAT);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.DOUBLE:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.DOUBLE);
                    } else {
                        ps.setDouble(i, Double.parseDouble(String.valueOf(f)));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.DOUBLE);
                    } catch (SQLException e1) {
                    }
                }
                break;
            case Types.LONGVARCHAR:
                try {
                    if (f == null) {
                        ps.setNull(i, Types.VARCHAR);
                    } else {
                        ps.setString(i, String.valueOf(f));
                    }
                } catch (Exception e) {
                    try {
                        ps.setNull(i, Types.VARCHAR);
                    } catch (SQLException e1) {
                    }
                }
                // 当数据库为mysql,字段类型为text,如果f值里面包含非数字类型转换会失败,为兼容采用Types.VARCHAR方式替换
                //			try {
                //                if (f == null) {
                //                    ps.setNull(i, Types.LONGVARCHAR);
                //                } else {
                //                    ps.setLong(i, Long.parseLong(String.valueOf(f)));
                //                }
                //            } catch (Exception e) {
                //                try {
                //                    ps.setNull(i, Types.LONGVARCHAR);
                //                } catch (SQLException e1) {
                //                }
                //            }
                break;
        }
    }

    /**
     * 获取数据库表元数据信息
     *
     * @param columnTypes
     * @return
     */
    public static List<String> getColumnTypeNames(int[] columnTypes) {
        if (columnTypes != null && columnTypes.length > 0) {
            List<String> columnTypeNames = new ArrayList<String>(columnTypes.length);
            for (int t : columnTypes) {
                columnTypeNames.add(getTypeName(t));
            }
            return columnTypeNames;
        }
        return null;
    }

    private static String getTypeName(int type) {
        switch (type) {
            case Types.BIT:
                return "BIT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.CHAR:
                return "CHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "BINARY";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.NULL:
                return "NULL";
            case Types.OTHER:
                return "OTHER";
            case Types.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case Types.DISTINCT:
                return "DISTINCT";
            case Types.STRUCT:
                return "STRUCT";
            case Types.ARRAY:
                return "ARRAY";
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
                return "CLOB";
            case Types.REF:
                return "REF";
            case Types.DATALINK:
                return "DATALINK";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.ROWID:
                return "ROWID";
            case Types.NCHAR:
                return "NCHAR";
            case Types.NVARCHAR:
                return "NVARCHAR";
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.NCLOB:
                return "NCLOB";
            case Types.SQLXML:
                return "SQLXML";
            case Types.REF_CURSOR:
                return "REF_CURSOR";
            case Types.TIME_WITH_TIMEZONE:
                return "TIME_WITH_TIMEZONE";
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP_WITH_TIMEZONE";
        }
        return null;
    }

}