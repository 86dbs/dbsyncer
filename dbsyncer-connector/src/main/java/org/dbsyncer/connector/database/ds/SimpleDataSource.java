package org.dbsyncer.connector.database.ds;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.util.DatabaseUtil;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class SimpleDataSource implements DataSource, AutoCloseable {

    private final BlockingQueue<SimpleConnection> pool = new LinkedBlockingQueue<>(300);
    /**
     * 有效期（毫秒），默认60s
     */
    private final long KEEP_ALIVE = 60000;
    /**
     * 有效检测时间（秒），默认10s
     */
    private final int VALID_TIMEOUT_SECONDS = 10;
    private String driverClassName;
    private String url;
    private String username;
    private String password;

    public SimpleDataSource(String driverClassName, String url, String username, String password) {
        this.driverClassName = driverClassName;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        SimpleConnection poll = pool.poll();
        if (null == poll) {
            return createConnection();
        }

        // 连接无效
        if (!poll.isValid(VALID_TIMEOUT_SECONDS)) {
            return createConnection();
        }

        // 连接过期
        if (isExpired(poll)) {
            return createConnection();
        }
        return poll;
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
        pool.forEach(c -> c.close());
    }

    public void close(Connection connection) {
        if (connection != null && connection instanceof SimpleConnection) {
            SimpleConnection simpleConnection = (SimpleConnection) connection;
            // 连接过期
            if (isExpired(simpleConnection)) {
                simpleConnection.close();
                return;
            }

            // 回收连接
            pool.offer(simpleConnection);
        }
    }

    /**
     * 连接是否过期
     *
     * @param connection
     * @return
     */
    private boolean isExpired(SimpleConnection connection) {
        return connection.getActiveTime() + KEEP_ALIVE < Instant.now().toEpochMilli();
    }

    /**
     * 创建新连接
     *
     * @return
     * @throws SQLException
     */
    private SimpleConnection createConnection() throws SQLException {
        return new SimpleConnection(DatabaseUtil.getConnection(driverClassName, url, username, password), StringUtil.equals(driverClassName, "oracle.jdbc.OracleDriver"));
    }

}