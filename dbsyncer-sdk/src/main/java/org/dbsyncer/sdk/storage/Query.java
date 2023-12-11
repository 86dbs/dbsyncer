/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage;

import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.filter.IntFilter;
import org.dbsyncer.sdk.storage.filter.StringFilter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public class Query {

    /**
     * {@link StorageEnum}
     */
    private StorageEnum type;

    private String metaId;

    private BooleanFilter booleanFilter = new BooleanFilter();

    /**
     * 查询应用性能，不用排序查询，只用查询总量即可
     */
    private boolean queryTotal;

    private int pageNum = 1;

    private int pageSize = 20;

    /**
     * 修改时间和创建默认降序返回
     */
    private SortEnum sort = SortEnum.DESC;

    /**
     * 返回值转换器，限Disk使用
     */
    private Map<String, FieldResolver> fieldResolverMap = new ConcurrentHashMap<>();

    public Query() {
    }

    public Query(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
    }

    public void addFilter(String name, String value) {
        booleanFilter.add(new StringFilter(name, FilterEnum.EQUAL, value, false));
    }

    public void addFilter(String name, String value, boolean enableHighLightSearch) {
        booleanFilter.add(new StringFilter(name, FilterEnum.LIKE, value, enableHighLightSearch));
    }

    public void addFilter(String name, int value) {
        booleanFilter.add(new IntFilter(name, value));
    }

    public StorageEnum getType() {
        return type;
    }

    public void setType(StorageEnum type) {
        this.type = type;
    }

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public BooleanFilter getBooleanFilter() {
        return booleanFilter;
    }

    public void setBooleanFilter(BooleanFilter booleanFilter) {
        this.booleanFilter = booleanFilter;
    }

    public boolean isQueryTotal() {
        return queryTotal;
    }

    public void setQueryTotal(boolean queryTotal) {
        this.queryTotal = queryTotal;
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

    public SortEnum getSort() {
        return sort;
    }

    public void setSort(SortEnum sort) {
        this.sort = sort;
    }

    public Map<String, FieldResolver> getFieldResolverMap() {
        return fieldResolverMap;
    }

    public void setFieldResolverMap(Map<String, FieldResolver> fieldResolverMap) {
        this.fieldResolverMap = fieldResolverMap;
    }

}