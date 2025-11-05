package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.math.BigDecimal;

/**
 * 无符号精确小数类型
 * 使用BigDecimal存储，但确保值 >= 0
 */
public abstract class UnsignedDecimalType extends AbstractDataType<BigDecimal> {

    protected UnsignedDecimalType() {
        super(BigDecimal.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_DECIMAL;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            // 确保值 >= 0
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        if (val instanceof String) {
            BigDecimal bd = new BigDecimal((String) val);
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        if (val instanceof Number) {
            BigDecimal bd = new BigDecimal(val.toString());
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new BigDecimal(b ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}

