package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

public abstract class IntType extends AbstractDataType<Integer> {

    protected IntType() {
        super(Integer.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.INT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.intValue();
        }
        if (val instanceof String) {
            return NumberUtil.toInt((String) val);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new Integer(b ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}
