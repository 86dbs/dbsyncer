package org.dbsyncer.connector;

import org.dbsyncer.sdk.spi.ConnectorMapper;

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
    protected abstract T convert(ConnectorMapper connectorMapper, Object val) throws Exception;

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
    public Object convertValue(ConnectorMapper connectorMapper, Object val) throws Exception {
        if (null != val) {
            // 是否需要跳过转换
            if (skipConvert(val)) {
                return val;
            }

            // 当数据类型不同时，返回转换值
            if (!val.getClass().equals(parameterClazz)) {
                return convert(connectorMapper, val);
            }
        }
        return getDefaultVal(val);
    }

}