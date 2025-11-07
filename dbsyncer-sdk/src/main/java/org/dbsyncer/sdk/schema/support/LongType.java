package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

public abstract class LongType extends AbstractDataType<Long> {

    protected LongType() {
        super(Long.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.LONG;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.longValue();
        }
        if (val instanceof String) {
            return NumberUtil.toLong((String) val);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new Long(b ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}
