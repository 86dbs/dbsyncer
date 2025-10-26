/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.connector.database.ds.SimpleDataSource;
import org.dbsyncer.sdk.util.DatabaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.util.Properties;

public class DatabaseConnectorInstance implements ConnectorInstance<DatabaseConfig, Connection> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DatabaseConfig config;
    private final SimpleDataSource dataSource;

    public DatabaseConnectorInstance(DatabaseConfig config) {
        this.config = config;
        Properties properties = DatabaseUtil.parseJdbcProperties(config.getProperties());
        if (StringUtil.isNotBlank(config.getUsername())) {
            properties.put("user", config.getUsername());
        }
        if (StringUtil.isNotBlank(config.getPassword())) {
            properties.put("password", config.getPassword());
        }
        this.dataSource = new SimpleDataSource(config.getDriverClassName(), config.getUrl(), properties, config.getMaxActive(), config.getKeepAlive());
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
            throw new SdkException(e.getMessage(), e.getCause());
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