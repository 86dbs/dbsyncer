package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class DecimalValueMapper extends AbstractValueMapper<BigDecimal> {

    @Override
    protected BigDecimal convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof Integer) {
            Integer integer = (Integer) val;
            return new BigDecimal(integer);
        }
        if (val instanceof Long) {
            Long l = (Long) val;
            return new BigDecimal(l);
        }
        if (val instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) val;
            return new BigDecimal(bigInteger);
        }
        if (val instanceof Short) {
            Short s = (Short) val;
            return new BigDecimal(s);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new BigDecimal(b ? 1 : 0);
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}