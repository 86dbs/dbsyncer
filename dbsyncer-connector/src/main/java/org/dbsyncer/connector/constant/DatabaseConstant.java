package org.dbsyncer.connector.constant;

public class DatabaseConstant {

    //*********************************** Mysql **************************************//
    /**
     * Mysql分页语句
     */
    public static final String MYSQL_PAGE_SQL = " LIMIT ?,?";

    /**
     * Mysql表列语句
     */
    public static final String MYSQL_TABLE_COLUMN_SQL = " LIMIT 0,0";

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
     * Oracle表列语句开始
     */
    public static final String ORACLE_TABLE_COLUMN_SQL_START = "SELECT A.* FROM (";

    /**
     * Oracle表列语句结束
     */
    public static final String ORACLE_TABLE_COLUMN_SQL_END = ")A WHERE ROWNUM < 1";

    //*********************************** SqlServer **************************************//
    /**
     * SqlServer分页语句开始
     */
    public static final String SQLSERVER_PAGE_SQL_START = " ORDER BY ";

    /**
     * SqlServer分页语句结束
     */
    public static final String SQLSERVER_PAGE_SQL_END = " OFFSET(?-1) * ? ROWS FETCH NEXT ? ROWS ONLY";

}