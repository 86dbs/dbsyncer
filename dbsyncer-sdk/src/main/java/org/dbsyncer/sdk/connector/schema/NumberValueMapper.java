package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class NumberValueMapper extends AbstractValueMapper<BigDecimal> {

    @Override
    protected BigDecimal convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Integer) {
            Integer integer = (Integer) val;
            return new BigDecimal(integer);
        }
        if (val instanceof Long) {
            Long l = (Long) val;
            return new BigDecimal(l);
        }
        if (val instanceof BigInteger) {
            BigInteger b = (BigInteger) val;
            return new BigDecimal(b);
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}