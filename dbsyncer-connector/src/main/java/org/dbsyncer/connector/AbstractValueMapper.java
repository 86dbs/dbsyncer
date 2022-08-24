package org.dbsyncer.connector;

import java.lang.reflect.ParameterizedType;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:34
 */
public abstract class AbstractValueMapper<T> implements ValueMapper {

    private final Class<T> parameterClazz;

    public AbstractValueMapper() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * 实现字段类型转换
     *
     * @param val
     */
    protected abstract Object convert(ConnectorMapper connectorMapper, Object val);

    @Override
    public Object convertValue(ConnectorMapper connectorMapper, Object val) {
        if (null == val) {
            return null;
        }

        // 当数据类型不同时，返回转换值
        return !val.getClass().equals(parameterClazz) ? convert(connectorMapper, val) : val;
    }
}