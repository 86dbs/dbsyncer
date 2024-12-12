package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;

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
        if (val instanceof String) {
            String s = (String) val;
            try {
                return new SerialBlob(s.getBytes(StandardCharsets.UTF_8));
            } catch (SQLException e) {
                throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
            }
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}