/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;

import java.lang.reflect.ParameterizedType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-24 20:58
 */
public abstract class AbstractDataType<T> implements DataType {

    private final Class<T> parameterClazz;
    protected ConnectorInstance connectorInstance;

    public AbstractDataType() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * 获取默认值
     *
     * @return null
     */
    protected T getDefaultVal() {
        return null;
    }

    protected T convertString(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertByte(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertShort(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertInt(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertLong(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertDecimal(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertDouble(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertFloat(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertBoolean(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertDate(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertTime(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertTimeStamp(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertBytes(Object val) {
        return throwUnsupportedException(val);
    }

    protected T convertArray(Object val) {
        return throwUnsupportedException(val);
    }

    protected T throwUnsupportedException(Object val) {
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), getType(), val));
    }

    protected T throwUnsupportedException(Object val, Field field) {
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val) {
        if (val == null) {
            return getDefaultVal();
        }

        // 数据类型匹配
        if (val.getClass().equals(parameterClazz)) {
            return val;
        }

        // 异构数据类型转换
        switch (getType()) {
            case STRING:
                return convertString(val);
            case BYTE:
                return convertByte(val);
            case SHORT:
                return convertShort(val);
            case INT:
                return convertInt(val);
            case LONG:
                return convertLong(val);
            case DECIMAL:
                return convertDecimal(val);
            case DOUBLE:
                return convertDouble(val);
            case FLOAT:
                return convertFloat(val);
            case BOOLEAN:
                return convertBoolean(val);
            case DATE:
                return convertDate(val);
            case TIME:
                return convertTime(val);
            case TIMESTAMP:
                return convertTimeStamp(val);
            case BYTES:
                return convertBytes(val);
            case ARRAY:
                return convertArray(val);
            default:
                return getDefaultVal();
        }
    }

    @Override
    public T convert(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }

    public void setConnectorInstance(ConnectorInstance connectorInstance) {
        this.connectorInstance = connectorInstance;
    }
}