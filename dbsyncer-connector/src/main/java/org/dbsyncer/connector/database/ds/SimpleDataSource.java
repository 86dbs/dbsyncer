package org.dbsyncer.connector.database.ds;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.logging.Logger;

public class SimpleDataSource implements DataSource, AutoCloseable {

    private final org.slf4j.Logger       logger = LoggerFactory.getLogger(getClass());
    private       LinkedList<SimpleConnection> pool   = new LinkedList<>();
    private       String                 url;
    private       String                 username;
    private       String                 password;
    private       String                 threanPoolName = "SimpleDataSourcePool-";
    private int minIdle = 10;
    private long lifeTime = 30 * 1000;

    public SimpleDataSource(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;

        try {
            for (int i = 0; i < minIdle; i++) {
                createConnection();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        // TODO 心跳检测过期连接
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (pool) {
            if (pool.isEmpty()) {
                createConnection();
            }
            return pool.getFirst();
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new ConnectorException("Unsupported method.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void close() {
        pool.forEach(c -> c.closeQuietly());
    }

    private void createConnection() throws SQLException {
        pool.addLast(new SimpleConnection(pool, DatabaseUtil.getConnection(url, username, password)));
    }

}