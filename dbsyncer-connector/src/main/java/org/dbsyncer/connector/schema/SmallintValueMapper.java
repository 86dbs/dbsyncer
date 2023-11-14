package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.sdk.spi.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class SmallintValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorMapper connectorMapper, Object val) {
        if(val instanceof Short){
            Short s = (Short) val;
            return new Integer(s);
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}