package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class RealValueMapper extends AbstractValueMapper<Float> {

    @Override
    protected Float convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof Double) {
            Double dob = (Double) val;
            return Float.valueOf(dob.floatValue());
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}