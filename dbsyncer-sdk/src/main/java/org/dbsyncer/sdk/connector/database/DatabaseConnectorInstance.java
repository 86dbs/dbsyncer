/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.connector.database.ds.SimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;

public class DatabaseConnectorInstance implements ConnectorInstance<DatabaseConfig, Connection> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DatabaseConfig config;
    private final SimpleDataSource dataSource;

    public DatabaseConnectorInstance(DatabaseConfig config) {
        this.config = config;
        this.dataSource = new SimpleDataSource(config.getDriverClassName(), config.getUrl(), config.getUsername(), config.getPassword(), config.getMaxActive(), config.getKeepAlive());
    }

    public <T> T execute(HandleCallback callback) throws Exception {
        Connection connection = null;
        try {
            connection = getConnection();
            return (T) callback.apply(new DatabaseTemplate((SimpleConnection) connection));
        } finally {
            dataSource.close(connection);
        }
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
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

    public SimpleDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}