/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public abstract class BooleanType extends AbstractDataType<Boolean> {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.BOOLEAN;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Boolean) {
            return val;
        }
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.intValue() == 1 ? Boolean.TRUE : Boolean.FALSE;
        }
        return throwUnsupportedException(val, field);
    }
}
