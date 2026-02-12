/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.model;

import java.util.List;
import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-12 12:12
 */
public class HttpProperties {

    /**
     * 系统参数，页数
     */
    private String pageNum;

    /**
     * 系统参数，页大小
     */
    private String pageSize;

    /**
     * 系统参数，游标参数
     */
    private List<String> cursors;

    /**
     * 普通参数
     */
    private Map<String, Object> params;

    public String getPageNum() {
        return pageNum;
    }

    public void setPageNum(String pageNum) {
        this.pageNum = pageNum;
    }

    public String getPageSize() {
        return pageSize;
    }

    public void setPageSize(String pageSize) {
        this.pageSize = pageSize;
    }

    public List<String> getCursors() {
        return cursors;
    }

    public void setCursors(List<String> cursors) {
        this.cursors = cursors;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
