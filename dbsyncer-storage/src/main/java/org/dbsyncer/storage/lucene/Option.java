package org.dbsyncer.storage.lucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.IndexFieldResolver;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @version 1.0.0
 * @Author AE86
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
    private Map<String, IndexFieldResolverEnum> indexFieldResolverMap = new ConcurrentHashMap<>();

    /**
     * 指定返回的值类型
     *
     * @param name
     * @return
     */
    public IndexFieldResolver getIndexFieldResolver(String name) {
        IndexFieldResolverEnum indexFieldResolverEnum = indexFieldResolverMap.get(name);
        if (null != indexFieldResolverEnum) {
            return indexFieldResolverEnum.getIndexFieldResolver();
        }
        return IndexFieldResolverEnum.STRING.getIndexFieldResolver();
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

    public Map<String, IndexFieldResolverEnum> getIndexFieldResolverMap() {
        return indexFieldResolverMap;
    }

    public void setIndexFieldResolverMap(Map<String, IndexFieldResolverEnum> indexFieldResolverMap) {
        this.indexFieldResolverMap = indexFieldResolverMap;
    }
}