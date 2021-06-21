package org.dbsyncer.connector.constant;

public class DatabaseConstant {

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
     * Oracle分页语句结束
     */
    public static final String ORACLE_PAGE_SQL_END = ")A WHERE ROWNUM <= ?) WHERE RN > ?";

    //*********************************** SqlServer **************************************//
    /**
     * SqlServer分页语句
     *
     * <pre>select w.* from my_org a,
     * (
     *     select top 4 t.* from
     * 		(
     * 			select top 10 s.* from my_org s order by id
     * 		) t
     * ) w where a.id = w.id order by a.id
     * </pre>
     */
    public static final String SQLSERVER_PAGE_SQL = "SELECT W.* FROM %s A, (SELECT TOP ? T.* FROM (SELECT TOP ? S.* FROM %s S ORDER BY %s) T) W WHERE A.%s = W.%s ORDER BY A.%s";

    /**
     * SqlServer分页语句开始(2012版本支持)
     */
    public static final String SQLSERVER_PAGE_SQL_START = " ORDER BY ";

    /**
     * SqlServer分页语句结束(2012版本支持)
     */
    public static final String SQLSERVER_PAGE_SQL_END = " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

}