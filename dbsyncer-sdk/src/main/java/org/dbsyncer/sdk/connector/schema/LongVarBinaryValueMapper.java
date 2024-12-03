package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.nio.charset.StandardCharsets;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class LongVarBinaryValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected Object getDefaultVal(Object val) {
        return null != val ? val : new byte[0];
    }

    @Override
    protected byte[] convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}