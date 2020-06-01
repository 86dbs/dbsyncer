package org.dbsyncer.storage.query;

import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.dbsyncer.common.util.CollectionUtils;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-01 22:57
 */
public class Option {

    private Set<String> highLightKeys;
    private boolean enableHighLightSearch;
    private SimpleHTMLFormatter formatter = null;

    public Option() {
    }

    public Option(Query query) {
        this.highLightKeys = query.getParams().stream()
                .filter(p -> p.isHighlighter())
                .map(p -> p.getKey())
                .collect(Collectors.toSet());
        if (!CollectionUtils.isEmpty(highLightKeys)) {
            enableHighLightSearch = true;
            formatter = new SimpleHTMLFormatter(query.getPreTag(), query.getPostTag());
        }
    }

    public Set<String> getHighLightKeys() {
        return highLightKeys;
    }

    public boolean isEnableHighLightSearch() {
        return enableHighLightSearch;
    }

    public SimpleHTMLFormatter getFormatter() {
        return formatter;
    }
}
