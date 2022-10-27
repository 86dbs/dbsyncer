package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class NumberValueMapper extends AbstractValueMapper<BigDecimal> {

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
            BigInteger b = (BigInteger) val;
            return new BigDecimal(b);
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}