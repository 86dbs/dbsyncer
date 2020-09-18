package org.dbsyncer.storage.query;

import org.dbsyncer.storage.enums.StorageEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 22:56
 */
public class Query {

    /**
     * {@link StorageEnum}
     */
    private StorageEnum type;

    private String collection;

    private List<Param> params;

    private int pageNum = 1;

    private int pageSize = 20;

    public Query() {
        this.params = new ArrayList<>();
    }

    public Query(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.params = new ArrayList<>();
    }

    public void put(String key, String value) {
        params.add(new Param(key, value));
    }

    public void put(String key, String value, boolean highlighter) {
        params.add(new Param(key, value, highlighter));
    }

    public StorageEnum getType() {
        return type;
    }

    public void setType(StorageEnum type) {
        this.type = type;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public List<Param> getParams() {
        return params;
    }

    public void setParams(List<Param> params) {
        this.params = params;
    }

    public int getPageNum() {
        return pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

}