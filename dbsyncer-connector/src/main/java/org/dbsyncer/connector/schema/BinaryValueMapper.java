package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class BinaryValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected Object getDefaultVal(Object val) {
        return null != val ? val : new byte[0];
    }

    @Override
    protected byte[] convert(ConnectorMapper connectorMapper, Object val) {
        if(val instanceof String){
            String s = (String) val;
            return s.getBytes();
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}