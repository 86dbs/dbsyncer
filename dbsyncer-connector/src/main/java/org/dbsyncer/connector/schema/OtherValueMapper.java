package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.database.DatabaseValueMapper;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.sql.Connection;
import java.sql.Struct;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/16 16:54
 */
public class OtherValueMapper extends AbstractValueMapper<Struct> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof String;
    }

    @Override
    protected Struct convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        if (val instanceof oracle.sql.STRUCT) {
            return (Struct) val;
        }
        // SqlServer Geometry
        if (val instanceof byte[]) {
            Object connection = connectorMapper.getConnection();
            if (connection instanceof Connection) {
                final DatabaseValueMapper mapper = new DatabaseValueMapper((SimpleConnection) connection);
                return mapper.getStruct((byte[]) val);
            }
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}