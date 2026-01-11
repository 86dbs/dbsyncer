/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.common.column;

import org.dbsyncer.common.util.StringUtil;

public abstract class AbstractColumnValue<T> implements ColumnValue {

    protected Object value;

    protected T getValue() {
        return (T) value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    /**
     * 检查值是否为空（null、空字符串或字符串"null"）
     */
    protected boolean isEmpty(String value) {
        return value == null || StringUtil.isBlank(value) || "null".equalsIgnoreCase(value);
    }

}