package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class BigintValueMapper extends AbstractValueMapper<Long> {

    @Override
    protected Long convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof BigDecimal) {
            BigDecimal bitDec = (BigDecimal) val;
            return bitDec.longValue();
        }
        if (val instanceof BigInteger) {
            BigInteger bitInt = (BigInteger) val;
            return bitInt.longValue();
        }
        if (val instanceof Integer) {
            return new Long((Integer) val);
        }
        if (val instanceof String) {
            return new Long((String) val);
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}