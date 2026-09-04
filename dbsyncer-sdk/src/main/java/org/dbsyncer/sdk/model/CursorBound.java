/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.StringUtil;

/**
 * 分片游标边界：起始（排他）到结束（含），以及是否表尾。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-04
 */
public class CursorBound {

    /**
     * 是否支持按游标划界；false 时调用方按整表一片处理
     */
    private boolean supported;

    /**
     * 起始游标（回显请求）
     */
    private String startCursor = "";

    /**
     * 结束游标（含）；无行时为空串
     */
    private String endCursor = "";

    /**
     * 是否最后一页（本窗后无更多行）
     */
    private boolean lastPage;

    /**
     * 本窗实际扫到的定位键行数；0 表示空表或无后续行
     */
    private int actualCount;

    /**
     * 不支持游标划界。
     *
     * @param startCursor 请求起始游标
     * @return 结果
     */
    public static CursorBound unsupported(String startCursor) {
        CursorBound bound = new CursorBound();
        bound.supported = false;
        bound.startCursor = StringUtil.getIfBlank(startCursor, StringUtil.EMPTY);
        bound.endCursor = StringUtil.EMPTY;
        bound.lastPage = true;
        bound.actualCount = 0;
        return bound;
    }

    /**
     * 支持游标划界的结果。
     *
     * @param startCursor 起始游标
     * @param endCursor   结束游标
     * @param lastPage    是否最后一页
     * @param actualCount 本窗行数
     * @return 结果
     */
    public static CursorBound of(String startCursor, String endCursor, boolean lastPage, int actualCount) {
        CursorBound bound = new CursorBound();
        bound.supported = true;
        bound.startCursor = StringUtil.getIfBlank(startCursor, StringUtil.EMPTY);
        bound.endCursor = StringUtil.getIfBlank(endCursor, StringUtil.EMPTY);
        bound.lastPage = lastPage;
        bound.actualCount = Math.max(actualCount, 0);
        return bound;
    }

    public boolean isSupported() {
        return supported;
    }

    public void setSupported(boolean supported) {
        this.supported = supported;
    }

    public String getStartCursor() {
        return startCursor;
    }

    public void setStartCursor(String startCursor) {
        this.startCursor = startCursor;
    }

    public String getEndCursor() {
        return endCursor;
    }

    public void setEndCursor(String endCursor) {
        this.endCursor = endCursor;
    }

    public boolean isLastPage() {
        return lastPage;
    }

    public void setLastPage(boolean lastPage) {
        this.lastPage = lastPage;
    }

    public int getActualCount() {
        return actualCount;
    }

    public void setActualCount(int actualCount) {
        this.actualCount = actualCount;
    }
}
