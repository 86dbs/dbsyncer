/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.Field;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 20:50
 */
public abstract class AbstractSchemaResolver implements SchemaResolver {

    private final Map<String, DataType> mapping = new ConcurrentHashMap<>();

    public AbstractSchemaResolver() {
        initDataTypeMapping(mapping);
        Assert.notEmpty(mapping, "At least one data type is required.");
    }

    protected abstract void initDataTypeMapping(Map<String, DataType> mapping);

    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        return mapping.get(field.getTypeName());
    }

    @Override
    public Object merge(Object val, Field field) {
        DataType dataType = getDataType(mapping, field);
        if (dataType != null) {
            return dataType.mergeValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] merge into [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val, Field field) {
        DataType dataType = getDataType(mapping, field);
        if (dataType != null) {
            return dataType.convertValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

}