package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.database.DatabaseTemplate;
import org.dbsyncer.connector.database.HandleCallback;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class SqlServerConnectorMapper extends DatabaseConnectorMapper {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lock   lock   = new ReentrantLock(true);

    public SqlServerConnectorMapper(DatabaseConfig config) throws SQLException {
        super(config);
    }

    /**
     * 使用连接时加锁(SqlServer 2008以下版本连接未释放问题)
     *
     * @param callback
     * @return
     */
    @Override
    public <T> T execute(HandleCallback callback) {
        final Lock connectionLock = lock;
        boolean locked = false;
        Object apply = null;
        Connection connection = null;
        try {
            locked = connectionLock.tryLock(60, TimeUnit.SECONDS);
            if (locked) {
                connection = DatabaseUtil.getConnection(getConfig());
                apply = callback.apply(new DatabaseTemplate(connection));
            }
        } catch (EmptyResultDataAccessException e) {
            throw e;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage());
        } finally {
            if (locked) {
                DatabaseUtil.close(connection);
                connectionLock.unlock();
            }
        }
        return (T) apply;
    }

}