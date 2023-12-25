package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

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
    protected byte[] convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            String s = (String) val;
            return s.getBytes();
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}