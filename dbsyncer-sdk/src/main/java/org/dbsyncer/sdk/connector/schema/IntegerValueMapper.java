package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class IntegerValueMapper extends AbstractValueMapper<Integer> {
    @Override
    protected Integer convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) val;
            return bigInteger.intValue();
        }

        if (val instanceof Long) {
            Long l = (Long) val;
            return l.intValue();
        }

        if (val instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) val;
            return bigDecimal.intValue();
        }

        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new Integer(b ? 1 : 0);
        }

        if (val instanceof Short) {
            Short s = (Short) val;
            return s.intValue();
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}