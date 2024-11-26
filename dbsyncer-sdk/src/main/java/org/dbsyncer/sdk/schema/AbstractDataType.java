/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.Field;

import java.lang.reflect.ParameterizedType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-24 20:58
 */
public abstract class AbstractDataType<T> implements DataType {

    private final Class<T> parameterClazz;
    protected SchemaResolver schemaResolver;

    public AbstractDataType() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * 转换为标准数据类型
     *
     * @param val
     * @param field
     * @return
     */
    protected abstract T merge(Object val, Field field);

    /**
     * 获取默认合并值
     *
     * @return
     */
    protected abstract T getDefaultMergedVal();

    /**
     * 转换为指定数据类型
     *
     * @param val
     * @param field
     * @return
     */
    protected abstract Object convert(Object val, Field field);

    /**
     * 获取默认转换值
     *
     * @return
     */
    protected abstract Object getDefaultConvertedVal();

    @Override
    public Object mergeValue(Object val, Field field) {
        if (val == null || field == null) {
            return getDefaultMergedVal();
        }
        // 数据类型匹配
        if (val.getClass().equals(parameterClazz)) {
            return val;
        }
        // 异构数据类型转换
        return merge(val, field);
    }

    @Override
    public Object convertValue(Object val, Field field) {
        if (val == null || field == null) {
            return getDefaultConvertedVal();
        }
        // 异构数据类型转换
        return convert(val, field);
    }

    protected T throwUnsupportedException(Object val, Field field) {
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    public void setSchemaResolver(SchemaResolver schemaResolver) {
        this.schemaResolver = schemaResolver;
    }
}