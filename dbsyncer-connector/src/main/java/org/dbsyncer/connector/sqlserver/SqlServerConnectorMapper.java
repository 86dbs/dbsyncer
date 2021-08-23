package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.database.HandleCallback;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.database.DatabaseTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class SqlServerConnectorMapper implements ConnectorMapper<ConnectorConfig, Connection> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lock   lock   = new ReentrantLock(true);

    private ConnectorConfig config;
    private Connection connection;

    public SqlServerConnectorMapper(ConnectorConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    /**
     * 使用连接时加锁(SqlServer 2008以下版本连接未释放问题)
     *
     * @param callback
     * @return
     */
    public <T> T execute(HandleCallback callback) {
        final Lock connectionLock = lock;
        boolean locked = false;
        Object apply = null;
        try {
            locked = connectionLock.tryLock(60, TimeUnit.SECONDS);
            if (locked) {
                apply = callback.apply(new DatabaseTemplate(connection));
            }
        } catch (EmptyResultDataAccessException e) {
            throw e;
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

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}