package org.dbsyncer.listener.sqlserver;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-05-18 21:14
 */
public class SqlServerExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void start() {
        try {
            final DatabaseConfig config = (DatabaseConfig) connectorConfig;
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();

        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        }
    }

    @Override
    public void close() {
    }

}