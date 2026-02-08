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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnectorInstance implements ConnectorInstance<DatabaseConfig, Connection> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DatabaseConfig config;
    private final SimpleDataSource dataSource;
    private final String catalog;
    private final String schema;

    public DatabaseConnectorInstance(DatabaseConfig config) {
        this(config, null, null);
    }

    public DatabaseConnectorInstance(DatabaseConfig config, String catalog, String schema) {
        this.config = config;
        this.catalog = catalog;
        this.schema = schema;
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
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
        Connection connection = dataSource.getConnection();
        // 动态设置数据库和模式
        if (connection instanceof SimpleConnection) {
            SimpleConnection simpleConnection = (SimpleConnection) connection;
            try {
                if (StringUtil.isNotBlank(catalog)) {
                    simpleConnection.setCatalog(catalog);
                }
                if (StringUtil.isNotBlank(schema)) {
                    simpleConnection.setSchema(schema);
                }
            } catch (SQLException e) {
                logger.warn("Failed to set catalog/schema: {}", e.getMessage());
                // 不抛出异常，允许连接继续使用
            }
        }
        return connection;
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
