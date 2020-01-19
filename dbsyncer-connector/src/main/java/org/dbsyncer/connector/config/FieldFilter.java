package org.dbsyncer.connector.config;

/**
 * 字段属性条件
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 15:10
 */
public class FieldFilter {

    /**
     * 字段名，ID
     */
    private String name;

    /**
     * 运算符，equal
     */
    private String operator;

    /**
     * 值
     */
    private String value;

    public FieldFilter() {
    }

    public FieldFilter(String name, String operator, String value) {
        this.name = name;
        this.operator = operator;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }
}