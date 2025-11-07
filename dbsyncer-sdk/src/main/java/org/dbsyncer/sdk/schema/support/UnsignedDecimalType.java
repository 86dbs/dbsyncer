package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;

import java.math.BigDecimal;

/**
 * 无符号精确小数类型
 * 使用BigDecimal存储，但确保值 >= 0
 */
public abstract class UnsignedDecimalType extends DecimalType {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_DECIMAL;
    }

    @Override
    protected Object convert(Object val, Field field) {
        Object result = super.convert(val, field);
        if (result instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) result;
            // 确保值 >= 0
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                throw new RuntimeException("data should be UNSIGNED");
            }
        }
        return result;
    }
}

