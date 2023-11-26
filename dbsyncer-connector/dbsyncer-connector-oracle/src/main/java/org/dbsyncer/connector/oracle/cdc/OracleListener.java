/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.dcn.DBChangeNotification;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public class OracleListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DBChangeNotification client;

    @Override
    public void start() {
        try {
            final DatabaseConfig config = (DatabaseConfig) connectorConfig;
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();
            client = new DBChangeNotification(username, password, url);
            client.setFilterTable(filterTable);
            client.addRowEventListener((e) -> sendChangedEvent(e));
            client.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new OracleException(e);
        }
    }

    @Override
    public void close() {
        if (null != client) {
            client.close();
        }
    }

}