package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class DoubleValueMapper extends AbstractValueMapper<Double> {

    @Override
    protected Double convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Number) {
            Number num = (Number) val;
            return num.doubleValue();
        }
        // 如果 val 是 String 类型，尝试将其转换为 Double
        if (val instanceof String) {
            return Double.parseDouble((String) val);
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}