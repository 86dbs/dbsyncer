package org.dbsyncer.connector.database.ds;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.util.DatabaseUtil;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class SimpleDataSource implements DataSource, AutoCloseable {

    private final BlockingQueue<SimpleConnection> pool = new LinkedBlockingQueue<>(2000);
    private long lifeTime = 60 * 1000;
    private String url;
    private String username;
    private String password;

    public SimpleDataSource(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (pool) {
            if (pool.isEmpty()) {
                pool.offer(new SimpleConnection(this, DatabaseUtil.getConnection(url, username, password)));
            }
            return pool.poll();
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

    public BlockingQueue<SimpleConnection> getPool() {
        return pool;
    }

    public long getLifeTime() {
        return lifeTime;
    }

    public void setLifeTime(long lifeTime) {
        this.lifeTime = lifeTime;
    }
}