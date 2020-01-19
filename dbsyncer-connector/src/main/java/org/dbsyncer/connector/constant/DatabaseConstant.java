package org.dbsyncer.connector.constant;

public class DatabaseConstant {

    //*********************************** Mysql **************************************//
    /**
     * Mysql分页语句
     */
    public static final String MYSQL_PAGE_SQL = " LIMIT ?,?";

    /**
     * Mysql驱动
     */
    public static final String MYSQL_DRIVER_CLASSNAME = "com.mysql.jdbc.Driver";

    //*********************************** Oracle **************************************//
    /**
     * Oracle分页语句开始
     */
    public static final String ORACLE_PAGE_SQL_START = "SELECT * FROM (SELECT A.*, ROWNUM RN FROM (";

    /**
     * Oracle分页语句结束
     */
    public static final String ORACLE_PAGE_SQL_END = ")A WHERE ROWNUM <= ?) WHERE RN > ?";

    /**
     * Oracle驱动
     */
    public static final String ORACLE_DRIVER_CLASSNAME = "oracle.jdbc.OracleDriver";

    //*********************************** SqlServer **************************************//
    /**
     * SqlServer分页语句开始
     */
    public static final String SQLSERVER_PAGE_SQL_START = " ORDER BY ";

    /**
     * SqlServer分页语句结束
     */
    public static final String SQLSERVER_PAGE_SQL_END = " OFFSET(?-1) * ? ROWS FETCH NEXT ? ROWS ONLY";

    /**
     * SqlServer驱动
     */
    public static final String SQLSERVER_DRIVER_CLASSNAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

}