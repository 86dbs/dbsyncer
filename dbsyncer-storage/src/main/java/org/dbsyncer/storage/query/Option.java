package org.dbsyncer.storage.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.dbsyncer.common.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-01 22:57
 */
public class Option {

    private Query       query;
    private Set<String> highLightKeys;
    private boolean     enableHighLightSearch;
    private Highlighter highlighter = null;

    public Option() {
    }

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
            enableHighLightSearch = true;
            SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
            highlighter = new Highlighter(formatter, new QueryScorer(query));
        }
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