/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import java.lang.reflect.ParameterizedType;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:34
 */
@Deprecated
public abstract class AbstractValueMapper<T> implements ValueMapper {

    private final Class<T> parameterClazz;
    private final Boolean enableCustomType;

    public AbstractValueMapper() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        enableCustomType = CustomType.class.equals(parameterClazz);
    }

    /**
     * 实现字段类型转换
     *
     * @param val
     */
    protected abstract T convert(ConnectorInstance connectorInstance, Object val) throws Exception;

    /**
     * 是否跳过类型转换
     *
     * @param val
     * @return
     */
    protected boolean skipConvert(Object val) {
        return false;
    }

    /**
     * 获取默认值
     *
     * @param val
     * @return
     */
    protected Object getDefaultVal(Object val) {
        return val;
    }

    @Override
    public Object convertValue(ConnectorInstance connectorInstance, Object val) throws Exception {
        if (null != val) {
            // 是否需要跳过转换
            if (skipConvert(val)) {
                return val;
            }

            // 是否使用自定义类型
            if (enableCustomType) {
                CustomType customType = (CustomType) convert(connectorInstance, val);
                return customType.getValue();
            }

            // 当数据类型不同时，返回转换值
            if (!val.getClass().equals(parameterClazz)) {
                return convert(connectorInstance, val);
            }
        }
        return getDefaultVal(val);
    }

}