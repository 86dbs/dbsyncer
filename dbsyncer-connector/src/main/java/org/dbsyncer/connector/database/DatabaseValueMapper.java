package org.dbsyncer.connector.database;

import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.database.ds.SimpleConnection;

import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

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
}