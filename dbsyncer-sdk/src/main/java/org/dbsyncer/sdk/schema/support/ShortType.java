package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

public abstract class ShortType extends AbstractDataType<Short> {

    protected ShortType() {
        super(Short.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.SHORT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return throwUnsupportedException(val, field);
    }
}
