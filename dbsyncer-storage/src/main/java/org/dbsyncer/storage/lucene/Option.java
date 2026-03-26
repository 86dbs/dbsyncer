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
}
