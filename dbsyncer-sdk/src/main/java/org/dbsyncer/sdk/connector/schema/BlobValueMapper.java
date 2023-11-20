package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.sql.Blob;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class BlobValueMapper extends AbstractValueMapper<Blob> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.BLOB || val instanceof byte[];
    }

    @Override
    protected Blob convert(ConnectorInstance connectorInstance, Object val) {
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}