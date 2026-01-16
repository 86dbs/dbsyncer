package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

public class OracleNClobValueMapper extends AbstractValueMapper<Object> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.NCLOB || val instanceof byte[];
    }

    @Override
    protected Object convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            return val;
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}
