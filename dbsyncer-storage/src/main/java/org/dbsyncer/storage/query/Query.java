package org.dbsyncer.storage.query;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 22:56
 */
public class Query {

    private List<Param> params;

    private boolean highlighter;

    private String preTag = "<span style='color:red'>";

    private String postTag = "</span>";

    private int pageNum = 1;

    private int pageSize = 20;

    public Query() {
        this.params = new ArrayList<>();
    }

    public Query(boolean highlighter, int pageNum, int pageSize) {
        this.highlighter = highlighter;
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.params = new ArrayList<>();
    }

    public void put(String key, String value) {
        params.add(new Param(key, value));
    }

    public List<Param> getParams() {
        return params;
    }

    public void setParams(List<Param> params) {
        this.params = params;
    }

    public boolean isHighlighter() {
        return highlighter;
    }

    public String getPreTag() {
        return preTag;
    }

    public String getPostTag() {
        return postTag;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

}