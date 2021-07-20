package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectorMapper {
    private final        Logger          logger          = LoggerFactory.getLogger(getClass());
    private              ConnectorConfig config;
    private              String          cacheKey;
    private              JdbcTemplate    jdbcTemplate;
    private              Connection      connection;
    private static final int             EXECUTE_TIMEOUT = 1;
    private final        Lock            lock            = new ReentrantLock(true);

    public ConnectorMapper(ConnectorConfig config, String cacheKey, JdbcTemplate jdbcTemplate) throws SQLException {
        this.config = config;
        this.cacheKey = cacheKey;
        this.jdbcTemplate = jdbcTemplate;
        this.connection = jdbcTemplate.getDataSource().getConnection();
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * 使用连接时加锁
     *
     * @param callback
     * @return
     */
    public Object execute(HandleCallback callback) {
        final Lock connectionLock = lock;
        boolean locked = false;
        Object apply = null;
        try {
            locked = connectionLock.tryLock(EXECUTE_TIMEOUT, TimeUnit.MICROSECONDS);
            if (locked) {
                logger.info("获取连接{}成功", cacheKey);
                apply = callback.apply(connection);
            } else {
                logger.info("连接{}正在使用中", cacheKey);
            }
        } catch (Exception e) {
            logger.error("获取连接{}异常", cacheKey);
            // nothing to do
        } finally {
            if (locked) {
                connectionLock.unlock();
                logger.info("释放连接{}成功", cacheKey);
            }
        }
        return apply;
    }

}