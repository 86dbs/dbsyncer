package org.dbsyncer.storage.query;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/17 23:56
 */
public class Param {
    private String key;
    private String value;
    private boolean highlighter;

    public Param(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public Param(String key, String value, boolean highlighter) {
        this.key = key;
        this.value = value;
        this.highlighter = highlighter;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isHighlighter() {
        return highlighter;
    }
}