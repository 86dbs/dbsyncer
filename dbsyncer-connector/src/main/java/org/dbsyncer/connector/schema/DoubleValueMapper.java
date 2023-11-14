package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

import java.math.BigDecimal;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class DoubleValueMapper extends AbstractValueMapper<Double> {

    @Override
    protected Double convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) val;
            return bigDecimal.doubleValue();
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}