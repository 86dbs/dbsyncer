package org.dbsyncer.storage.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.IndexFieldResolver;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private Map<String, IndexFieldResolverEnum> fieldResolvers = new LinkedHashMap<>();

    public Option(Query query) {
        this.query = query;
    }

    public Option(Query query, List<Param> params) {
        this.query = query;
        if (!CollectionUtils.isEmpty(params)) {
            this.highLightKeys = params.stream()
                    .filter(p -> p.isHighlighter())
                    .map(p -> p.getKey())
                    .collect(Collectors.toSet());
        }
        if (!CollectionUtils.isEmpty(highLightKeys)) {
            this.enableHighLightSearch = true;
            SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
            highlighter = new Highlighter(formatter, new QueryScorer(query));
        }
    }

    public IndexFieldResolver getFieldResolver(String name){
        if(fieldResolvers.containsKey(name)){
            return fieldResolvers.get(name).getIndexFieldResolver();
        }
        return IndexFieldResolverEnum.STRING.getIndexFieldResolver();
    }

    public void addIndexFieldResolverEnum(String name, IndexFieldResolverEnum fieldResolver){
        fieldResolvers.putIfAbsent(name, fieldResolver);
    }

    public Query getQuery() {
        return query;
    }

    public Set<String> getHighLightKeys() {
        return highLightKeys;
    }

    public boolean isEnableHighLightSearch() {
        return enableHighLightSearch;
    }

    public Highlighter getHighlighter() {
        return highlighter;
    }
}