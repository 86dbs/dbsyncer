package org.dbsyncer.connector.database;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.database.ds.SimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public class DatabaseConnectorMapper implements ConnectorMapper<DatabaseConfig, Connection> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DatabaseConfig config;
    private SimpleDataSource dataSource;

    public DatabaseConnectorMapper(DatabaseConfig config) {
        this.config = config;
        this.dataSource = new SimpleDataSource(config.getDriverClassName(), config.getUrl(), config.getUsername(), config.getPassword());
    }

    public <T> T execute(HandleCallback callback) {
        Connection connection = null;
        try {
            connection = getConnection();
            return (T) callback.apply(new DatabaseTemplate((SimpleConnection) connection));
        } catch (EmptyResultDataAccessException e) {
            throw e;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ConnectorException(e.getMessage(), e.getCause());
        } finally {
            dataSource.close(connection);
        }
    }

    @Override
    public DatabaseConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
