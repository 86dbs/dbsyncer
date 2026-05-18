/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.lucene;

import org.dbsyncer.sdk.filter.FieldResolver;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-06-01 22:57
 */
public class Option {

    private Query query;

    private Set<String> highLightKeys;

    private boolean enableHighLightSearch;

    private Highlighter highlighter = null;

    /**
     * 只查总数
     */
    private boolean queryTotal;

    /**
     * 返回值转换器
     */
    private Map<String, FieldResolver> fieldResolverMap = new ConcurrentHashMap<>();

    /**
     * 返回字段白名单（与 {@link org.dbsyncer.sdk.filter.Query#getSelectFlied()} 一致，驼峰字段名）。
     * 为空时表示返回文档全部存储字段。
     */
    private Set<String> selectFields;

    /**
     * 指定返回的值类型
     *
     * @param name
     * @return
     */
    public FieldResolver<IndexableField> getFieldResolver(String name) {
        if (fieldResolverMap.containsKey(name)) {
            return fieldResolverMap.get(name);
        }
        return (f)->f.stringValue();
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Set<String> getHighLightKeys() {
        return highLightKeys;
    }

    public void setHighLightKeys(Set<String> highLightKeys) {
        this.highLightKeys = highLightKeys;
    }

    public boolean isEnableHighLightSearch() {
        return enableHighLightSearch;
    }

    public void setEnableHighLightSearch(boolean enableHighLightSearch) {
        this.enableHighLightSearch = enableHighLightSearch;
    }

    public Highlighter getHighlighter() {
        return highlighter;
    }

    public void setHighlighter(Highlighter highlighter) {
        this.highlighter = highlighter;
    }

    public boolean isQueryTotal() {
        return queryTotal;
    }

    public void setQueryTotal(boolean queryTotal) {
        this.queryTotal = queryTotal;
    }

    public Map<String, FieldResolver> getFieldResolverMap() {
        return fieldResolverMap;
    }

    public void setFieldResolverMap(Map<String, FieldResolver> fieldResolverMap) {
        this.fieldResolverMap = fieldResolverMap;
    }

    public Set<String> getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(Set<String> selectFields) {
        this.selectFields = selectFields;
    }

    /**
     * 是否在结果中包含该存储字段（自定义 SELECT 白名单）。
     *
     * @param name Lucene 字段名（与索引一致，一般为驼峰）
     * @return 未配置白名单时为 true
     */
    public boolean includeField(String name) {
        return selectFields == null || selectFields.isEmpty() || selectFields.contains(name);
    }
}
