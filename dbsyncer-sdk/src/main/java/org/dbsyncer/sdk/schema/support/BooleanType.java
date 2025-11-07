package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.schema.AbstractDataType;

public abstract class BooleanType extends AbstractDataType<Boolean> {

    protected BooleanType() {
        super(Boolean.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.BOOLEAN;
    }
}
