/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.lang.reflect.ParameterizedType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-24 20:58
 */
public abstract class AbstractDataType<T> implements DataType {

    private static final long serialVersionUID = 1L;
    private final Class<T> parameterClazz;
    protected ConnectorInstance connectorInstance;

    public AbstractDataType() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected T convertString(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertByte(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertShort(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertInt(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertLong(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertDecimal(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertDouble(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertFloat(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertBoolean(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertDate(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertTime(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertTimeStamp(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertBytes(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T convertArray(DataType dataType, Object val) {
        return throwUnsupportedException(dataType, val);
    }

    protected T throwUnsupportedException(DataType dataType, Object val) {
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), dataType.getType(), val));
    }

    /**
     * 获取默认值
     *
     * @return null
     */
    protected Object getDefaultVal() {
        return null;
    }

    @Override
    public Object convert(DataType dataType, Object val) {
        if (dataType == null || val == null) {
            return getDefaultVal();
        }

        // 数据类型匹配
        if (val.getClass().equals(parameterClazz)) {
            return val;
        }

        // 异构数据类型转换
        switch (dataType.getType()) {
            case STRING:
                return convertString(dataType, val);
            case BYTE:
                return convertByte(dataType, val);
            case SHORT:
                return convertShort(dataType, val);
            case INT:
                return convertInt(dataType, val);
            case LONG:
                return convertLong(dataType, val);
            case DECIMAL:
                return convertDecimal(dataType, val);
            case DOUBLE:
                return convertDouble(dataType, val);
            case FLOAT:
                return convertFloat(dataType, val);
            case BOOLEAN:
                return convertBoolean(dataType, val);
            case DATE:
                return convertDate(dataType, val);
            case TIME:
                return convertTime(dataType, val);
            case TIMESTAMP:
                return convertTimeStamp(dataType, val);
            case BYTES:
                return convertBytes(dataType, val);
            case ARRAY:
                return convertArray(dataType, val);
            default:
                return getDefaultVal();
        }
    }

    public ConnectorInstance getConnectorInstance() {
        return connectorInstance;
    }

    public void setConnectorInstance(ConnectorInstance connectorInstance) {
        this.connectorInstance = connectorInstance;
    }
}