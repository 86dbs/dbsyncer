/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.common.util.StringUtil;

import java.util.Collection;

/**
 * @author 穿云
 * @version 1.0.0
 * @time 2026-01-18 12:48
 */
public abstract class CustomData {

    private final Object value;

    public CustomData(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public abstract Collection<?> apply();

    @Override
    public String toString() {
        return value == null ? StringUtil.EMPTY : String.valueOf(value);
    }
}
