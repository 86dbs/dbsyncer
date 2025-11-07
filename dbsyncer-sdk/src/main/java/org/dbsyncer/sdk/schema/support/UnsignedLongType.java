package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.math.BigDecimal;

/**
 * 无符号长整型 (0-18446744073709551615)
 * 使用BigDecimal存储以避免溢出
 */
public abstract class UnsignedLongType extends AbstractDataType<BigDecimal> {

    protected UnsignedLongType() {
        super(BigDecimal.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_LONG;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            return new BigDecimal((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}

