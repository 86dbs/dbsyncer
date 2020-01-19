package org.dbsyncer.storage.query;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/17 23:56
 */
public class Param {
    private String key;
    private Object value;

    public Param(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }
}
