/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import org.dbsyncer.common.util.NumberUtil;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 游标分页结果（cursor 为字符串标识，兼容数字偏移游标）
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-15 13:46
 */
public class CursorPageQuery<T> {

    private long total;
    /**
     * 游标（可存数字偏移或业务游标串）
     */
    private String cursor;
    private int pageSize;
    private boolean hasMore;
    private Collection<T> data = new ArrayList<>();

    public CursorPageQuery() {
    }

    /**
     * 填充分页字段并计算 hasMore（参数由调用方保证有效，不做默认值兜底）
     *
     * @param data     当前页数据
     * @param total    总条数
     * @param cursor   游标
     * @param pageSize 每页条数
     */
    protected void fill(Collection<T> data, long total, String cursor, int pageSize) {
        this.data = data;
        this.total = total;
        this.cursor = cursor;
        this.pageSize = pageSize;
        long from = Math.min(NumberUtil.toLong(cursor, 0L), this.total);
        long to = Math.min(from + this.pageSize, this.total);
        this.hasMore = to < this.total;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    public Collection<T> getData() {
        return data;
    }

    public void setData(Collection<T> data) {
        this.data = data;
    }
}
