package org.dbsyncer.common.model;

import java.util.Collection;
import java.util.Collections;

public class Paging {

    private long          total;
    private int           pageNum;
    private int           pageSize;
    private Collection data;

    public Paging(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.data = Collections.EMPTY_LIST;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
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

    public Collection getData() {
        return data;
    }

    public void setData(Collection data) {
        this.data = data;
    }
}