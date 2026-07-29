/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

/**
 * 原生 SQL 查询参数（可选分页）。
 * <p>{@code pageNum/pageSize} 均大于 0 时按分页查询，否则全量返回。
 *
 * @author wuji
 * @version 1.0.0
 */
public class SqlQuery {

    private String sql;

    private Object[] args = new Object[0];

    private int pageNum;

    private int pageSize;

    private SqlQuery() {
    }

    /**
     * 构建无分页查询。
     *
     * @param sql  SQL（参数用 ? 占位）
     * @param args 绑定参数
     * @return 查询参数
     */
    public static SqlQuery of(String sql, Object... args) {
        SqlQuery query = new SqlQuery();
        query.sql = sql;
        query.args = args == null ? new Object[0] : args;
        return query;
    }

    /**
     * 设置分页（pageNum/pageSize 均需 &gt; 0 才生效）。
     *
     * @param pageNum  页码（从 1 起）
     * @param pageSize 每页条数
     * @return this
     */
    public SqlQuery page(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        return this;
    }

    /**
     * 是否启用分页。
     *
     * @return true 表示按分页查询
     */
    public boolean isPaged() {
        return pageNum > 0 && pageSize > 0;
    }

    public String getSql() {
        return sql;
    }

    public Object[] getArgs() {
        return args;
    }

    public int getPageNum() {
        return pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }
}
