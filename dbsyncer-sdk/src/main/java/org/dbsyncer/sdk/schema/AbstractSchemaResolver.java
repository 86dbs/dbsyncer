/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.ConnectorInstance;
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

    private final Map<String, DataType> mapping = new LinkedHashMap<>();
    private ConnectorInstance connectorInstance;

    public AbstractSchemaResolver() {
        initDataTypeMapping(mapping);
        Assert.notEmpty(mapping, "At least one data type is required.");
    }

    protected abstract void initDataTypeMapping(Map<String, DataType> mapping);

    @Override
    public Object merge(Object val, Field field) {
        if (mapping.containsKey(field.getTypeName())) {
            return mapping.get(field.getTypeName()).mergeValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] merge into [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val, Field field) {
        if (mapping.containsKey(field.getTypeName())) {
            return mapping.get(field.getTypeName()).convertValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }

    public void setConnectorInstance(ConnectorInstance connectorInstance) {
        this.connectorInstance = connectorInstance;
    }
}