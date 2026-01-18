package org.dbsyncer.sdk.constant;

public class DatabaseConstant {

    /**
     * dbs唯一标识码
     */
    public static final String DBS_UNIQUE_CODE = "/*dbs*/";

    //*********************************** Mysql **************************************//
    /**
     * Mysql分页语句
     */
    public static final String MYSQL_PAGE_SQL = " LIMIT ?,?";

    //*********************************** Oracle **************************************//
    /**
     * Oracle分页语句开始
     */
    public static final String ORACLE_PAGE_SQL_START = "SELECT * FROM (SELECT A.*, ROWNUM RN FROM (";

    /**
     * Oracle分页语句开始
     */
    public static final String ORACLE_PAGE_SQL_END = ") A WHERE ROWNUM <= ?) WHERE RN > ?";

    /**
     * Oracle游标分页语句开始
     */
    public static final String ORACLE_PAGE_CURSOR_SQL_START = "SELECT * FROM (SELECT A.* FROM (";

    /**
     * Oracle游标分页语句结束
     */
    public static final String ORACLE_PAGE_CURSOR_SQL_END = ")A WHERE ROWNUM <= ?)";

    //*********************************** SqlServer **************************************//
    /**
     * SqlServer分页语句(2008版本支持)
     * <pre>
     *  select * from (
     *      select row_number() over(order by id) as sqlserver_row_id, * from (select * from my_user) s
     *  ) as a where a.sqlserver_row_id between 1 and 10
     * </pre>
     */
    public static final String SQLSERVER_PAGE_SQL = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY %s) AS SQLSERVER_ROW_ID, * FROM (%s) S) A WHERE A.SQLSERVER_ROW_ID BETWEEN ? AND ?";

    //*********************************** PostgreSQL **************************************//
    /**
     * PostgreSQL分页语句
     */
    public static final String POSTGRESQL_PAGE_SQL = " limit ? OFFSET ?";

    //*********************************** SQLite **************************************//
    /**
     * SQLite分页语句
     */
    public static final String SQLITE_PAGE_SQL = " limit ? OFFSET ?";
}