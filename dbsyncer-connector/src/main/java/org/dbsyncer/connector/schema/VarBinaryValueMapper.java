package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class VarBinaryValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected Object getDefaultVal(Object val) {
        return null != val ? val : new byte[0];
    }

    @Override
    protected byte[] convert(ConnectorMapper connectorMapper, Object val) {
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}