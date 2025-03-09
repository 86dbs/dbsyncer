/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.math.BigDecimal;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public abstract class DecimalType extends AbstractDataType<BigDecimal> {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.DECIMAL;
    }
    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return new BigDecimal((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}
