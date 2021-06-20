package org.dbsyncer.connector.database.setter;

import oracle.jdbc.OracleConnection;
import oracle.sql.NCLOB;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;

public class PreparedFieldMapper {

    private JdbcTemplate jdbcTemplate;

    public PreparedFieldMapper(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public NClob getNClob(byte[] bytes) throws SQLException {
        Connection connection = null;
        try {
            connection = jdbcTemplate.getDataSource().getConnection();
            if (connection instanceof OracleConnection) {
                OracleConnection conn = (OracleConnection) connection;
                return new NCLOB(conn, bytes);
            }
            return connection.createNClob();
        } finally {
            DatabaseUtil.close(connection);
        }
    }

}
