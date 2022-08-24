package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.database.setter.PreparedFieldMapper;

import java.sql.Blob;
import java.sql.Connection;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class BlobValueMapper extends AbstractValueMapper<Blob> {

    @Override
    protected Blob convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        if (val instanceof byte[]) {
            Object connection = connectorMapper.getConnection();
            if (connection instanceof Connection) {
                final PreparedFieldMapper mapper = new PreparedFieldMapper((Connection) connection);
                return mapper.getBlob((byte[]) val);
            }
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}