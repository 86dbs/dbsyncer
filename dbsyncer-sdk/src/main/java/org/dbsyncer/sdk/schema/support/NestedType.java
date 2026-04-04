/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2026-04-02 14:25
 */
public abstract class NestedType extends AbstractDataType<Object> {

    @Override
    protected Object convert(Object val, Field field) {
        return val;
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.RELTABLE;
    }
}
