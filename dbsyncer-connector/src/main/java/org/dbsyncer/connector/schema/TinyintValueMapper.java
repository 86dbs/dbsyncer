package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class TinyintValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorMapper connectorMapper, Object val) {
        if(val instanceof Short){
            Short s = (Short) val;
            return new Integer(s);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return new Integer(b ? 1 : 0);
        }
        if (val instanceof String) {
            String s = (String) val;
            return new Integer(s);
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}