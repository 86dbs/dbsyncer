package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Clob;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
@Deprecated
public class ClobValueMapper extends AbstractValueMapper<Clob> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.CLOB || val instanceof byte[] || val instanceof String;
    }

    @Override
    protected Clob convert(ConnectorInstance connectorInstance, Object val) {
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}