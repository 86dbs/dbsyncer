package org.dbsyncer.listener.oracle;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.listener.AbstractDatabaseExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.oracle.dcn.DBChangeNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class OracleExtractor extends AbstractDatabaseExtractor {

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
            throw new ListenerException(e);
        }
    }

    @Override
    public void close() {
        if (null != client) {
            client.close();
        }
    }

}