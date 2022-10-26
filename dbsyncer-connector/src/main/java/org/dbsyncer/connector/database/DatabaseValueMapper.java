package org.dbsyncer.connector.database;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
//import oracle.spatial.geometry.JGeometry;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.nio.charset.Charset;
import java.sql.*;

public class DatabaseValueMapper {

    private SimpleConnection connection;

    public DatabaseValueMapper(SimpleConnection connection) {
        this.connection = connection;
    }

    public NClob getNClob(byte[] bytes) throws SQLException {
        if (connection.getConnection() instanceof OracleConnection) {
            OracleConnection conn = (OracleConnection) connection.getConnection();
            NClob nClob = conn.createNClob();
            nClob.setString(1, new String(bytes, Charset.defaultCharset()));
            return nClob;
        }
        return connection.createNClob();
    }

    public Blob getBlob(byte[] bytes) throws SQLException {
        if (connection.getConnection() instanceof OracleConnection) {
            OracleConnection conn = (OracleConnection) connection.getConnection();
            Blob blob = conn.createBlob();
            blob.setBytes(1, bytes);
            return blob;
        }
        return connection.createBlob();
    }

    public Clob getClob(byte[] bytes) throws SQLException {
        if (connection.getConnection() instanceof OracleConnection) {
            OracleConnection conn = (OracleConnection) connection.getConnection();
            Clob clob = conn.createClob();
            clob.setString(1, new String(bytes, Charset.defaultCharset()));
            return clob;
        }
        return connection.createClob();
    }

    public Struct getStruct(byte[] val) throws SQLException {
        return null;
//        if (connection.getConnection() instanceof OracleConnection) {
//            OracleConnection conn = connection.unwrap(OracleConnection.class);
//            Geometry geometry = Geometry.deserialize(val);
//            Double x = geometry.getX();
//            Double y = geometry.getY();
//            JGeometry jGeometry = new JGeometry(x, y, 0);
//            return JGeometry.store(jGeometry, conn);
//        }
//        throw new ConnectorException(String.format("%s can not get STRUCT [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}