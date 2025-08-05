package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.math.BigDecimal;

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
        else if (val instanceof String) {
            String str = (String) val;
            try {
                return Double.parseDouble(str);
            } catch (NumberFormatException e) {
                throw new SdkException(String.format("%s can not convert String [%s] to Double", getClass().getSimpleName(), str), e);
            }
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}