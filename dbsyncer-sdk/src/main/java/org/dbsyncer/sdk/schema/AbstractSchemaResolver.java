/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.Field;
import org.springframework.util.Assert;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 20:50
 */
public abstract class AbstractSchemaResolver implements SchemaResolver {

    private final Map<String, DataType> map = new LinkedHashMap<>();

    public AbstractSchemaResolver() {
        initDataTypes(map);
        Assert.notEmpty(map, "At least one data type is required.");
    }

    protected abstract void initDataTypes(Map<String, DataType> map);

    @Override
    public Object merge(Object val, Field field) {
        if (map.containsKey(field.getTypeName())) {
            return map.get(field.getTypeName()).mergeValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] merge into [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val, Field field) {
        if (map.containsKey(field.getTypeName())) {
            return map.get(field.getTypeName()).convertValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }
}