package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.database.DatabaseValueMapper;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.sql.Clob;
import java.sql.Connection;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class ClobValueMapper extends AbstractValueMapper<Clob> {

    @Override
    protected Clob convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        if (val instanceof byte[]) {
            Object connection = connectorMapper.getConnection();
            if (connection instanceof Connection) {
                final DatabaseValueMapper mapper = new DatabaseValueMapper((SimpleConnection) connection);
                return mapper.getClob((byte[]) val);
            }
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}