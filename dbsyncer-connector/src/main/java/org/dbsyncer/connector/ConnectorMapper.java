package org.dbsyncer.connector;

import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.database.DatabaseTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectorMapper {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lock lock = new ReentrantLock(true);
    protected ConnectorConfig config;
    protected Connection connection;

    public ConnectorMapper(ConnectorConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    public ConnectorConfig getConfig() {
        return config;
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
    public <T> T execute(HandleCallback callback) {
        final Lock connectionLock = lock;
        boolean locked = false;
        Object apply = null;
        try {
            locked = connectionLock.tryLock();
            if (locked) {
                apply = callback.apply(new DatabaseTemplate(connection));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        } finally {
            if (locked) {
                connectionLock.unlock();
            }
        }
        return (T) apply;
    }

}