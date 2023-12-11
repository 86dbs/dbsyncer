/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.common.column;

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

}