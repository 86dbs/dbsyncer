package org.dbsyncer.connector.database;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
import oracle.spatial.geometry.JGeometry;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.sql.SQLException;
import java.sql.Struct;

public class DatabaseValueMapper {

    private SimpleConnection connection;

    public DatabaseValueMapper(SimpleConnection connection) {
        this.connection = connection;
    }

    public Struct getStruct(byte[] val) throws SQLException {
        if (connection.getConnection() instanceof OracleConnection) {
            OracleConnection conn = connection.unwrap(OracleConnection.class);
            Geometry geometry = Geometry.deserialize(val);
            Double x = geometry.getX();
            Double y = geometry.getY();
            JGeometry jGeometry = new JGeometry(x, y, 0);
            return JGeometry.store(jGeometry, conn);
        }
        throw new ConnectorException(String.format("%s can not get STRUCT [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}