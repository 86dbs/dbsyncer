/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

/**
 * 自定义转换类型
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-04-08 23:17
 */
@Deprecated
public final class CustomType {

    private final Object value;

    public CustomType(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}