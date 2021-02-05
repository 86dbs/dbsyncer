package org.dbsyncer.connector.config;

import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;

/**
 * 字段属性条件
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 15:10
 */
public class Filter {

    /**
     * 字段名，ID
     */
    private String name;

    /**
     * @see OperationEnum
     */
    private String operation;

    /**
     * @see FilterEnum
     */
    private String filter;

    /**
     * 值
     */
    private String value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}