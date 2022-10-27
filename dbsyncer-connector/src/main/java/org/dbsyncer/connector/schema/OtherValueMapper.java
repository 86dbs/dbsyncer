package org.dbsyncer.connector.schema;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
import oracle.spatial.geometry.JGeometry;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
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
        return val instanceof oracle.sql.STRUCT || val instanceof String;
    }

    @Override
    protected Struct convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        // SqlServer Geometry
        if (val instanceof byte[]) {
            Object connection = connectorMapper.getConnection();
            if (connection instanceof Connection) {
                SimpleConnection simpleConnection = (SimpleConnection) connection;
                if (simpleConnection instanceof OracleConnection) {
                    OracleConnection conn = simpleConnection.unwrap(OracleConnection.class);
                    Geometry geometry = Geometry.deserialize((byte[]) val);
                    Double x = geometry.getX();
                    Double y = geometry.getY();
                    JGeometry jGeometry = new JGeometry(x, y, 0);
                    return JGeometry.store(jGeometry, conn);
                }
            }
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}