package org.dbsyncer.connector.database.setter;

import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.nio.charset.Charset;
import java.sql.*;

public class PreparedFieldMapper {

    private SimpleConnection connection;

    public PreparedFieldMapper(Connection connection) {
        this.connection = (SimpleConnection) connection;
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
}