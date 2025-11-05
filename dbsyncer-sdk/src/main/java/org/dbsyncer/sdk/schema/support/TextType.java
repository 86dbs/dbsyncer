package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

public abstract class TextType extends AbstractDataType<String> {

    protected TextType() {
        super(String.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.TEXT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return throwUnsupportedException(val, field);
    }
}

