/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.ds;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.util.DatabaseUtil;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class SimpleDataSource implements DataSource, AutoCloseable {

    /**
     * 默认最大连接
     */
    private final int MAX_IDLE = 300;

    /**
     * 连接上限后最大等待时间（秒）
     */
    private final int MAX_WAIT_SECONDS = 3;

    /**
     * 从缓存队列获取连接次数
     */
    private final int MAX_PULL_TIME = 20;

    /**
     * 活跃连接数
     */
    private AtomicInteger activeNum = new AtomicInteger(0);

    private final ReentrantLock lock = new ReentrantLock();

    private final BlockingQueue<SimpleConnection> pool = new LinkedBlockingQueue<>(MAX_IDLE);
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
        try {
            lock.lock();
            //如果当前连接数大于或等于最大连接数
            if (activeNum.get() >= MAX_IDLE) {
                //等待3秒
                TimeUnit.SECONDS.sleep(MAX_WAIT_SECONDS);
                if (activeNum.get() >= MAX_IDLE) {
                    throw new SdkException(String.format("数据库连接数超过上限%d，url=%s", MAX_IDLE, url));
                }
            }
            int time = MAX_PULL_TIME;
            while (time-- > 0){
                SimpleConnection poll = pool.poll();
                if (null == poll) {
                    return createConnection();
                }
                // 连接无效或过期, 直接关闭连接
                if (!poll.isValid(VALID_TIMEOUT_SECONDS) || isExpired(poll)) {
                    closeQuietly(poll);
                    continue;
                }
                // 返回缓存连接
                return poll;
            }

            // 兜底方案，保证一定能获取连接
            return createConnection();
        } catch (InterruptedException e) {
            throw new SdkException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SdkException("Unsupported method.");
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
                closeQuietly(simpleConnection);
                return;
            }

            // 回收连接
            pool.offer(simpleConnection);
        }
    }

    private void closeQuietly(SimpleConnection connection) {
        if (connection != null) {
            connection.close();
            activeNum.decrementAndGet();
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
        SimpleConnection simpleConnection = null;
        try {
            simpleConnection = new SimpleConnection(DatabaseUtil.getConnection(driverClassName, url, username, password), StringUtil.equals(driverClassName, "oracle.jdbc.OracleDriver"));
            activeNum.incrementAndGet();
        } catch (SQLException e) {
            throw new SdkException(e);
        }
        return simpleConnection;
    }

}