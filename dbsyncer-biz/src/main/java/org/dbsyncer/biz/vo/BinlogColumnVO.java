package org.dbsyncer.biz.vo;

public class BinlogColumnVO {

    private String key;

    private Object value;

    private String keyType;

    private String valueType;

    public BinlogColumnVO(String key, Object value, String keyType) {
        this.key = key;
        this.value = value;
        this.keyType = keyType;
        this.valueType = value == null ? "" : value.getClass().getName();
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public String getKeyType() {
        return keyType;
    }

    public String getValueType() {
        return valueType;
    }
}
