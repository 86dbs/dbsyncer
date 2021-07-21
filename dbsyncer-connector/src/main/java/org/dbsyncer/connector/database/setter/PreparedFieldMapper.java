package org.dbsyncer.connector.database.setter;

import oracle.jdbc.OracleConnection;
import oracle.sql.NCLOB;

import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;

public class PreparedFieldMapper {

    private Connection connection;

    public PreparedFieldMapper(Connection connection) {
        this.connection = connection;
    }

    public NClob getNClob(byte[] bytes) throws SQLException {
        if (connection instanceof OracleConnection) {
            OracleConnection conn = (OracleConnection) connection;
            return new NCLOB(conn, bytes);
        }
        return connection.createNClob();
    }

}
