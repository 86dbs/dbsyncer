package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:40
 */
public class MessageDecoderContext {
    private DatabaseConfig config;
    private String publicationName;

    public MessageDecoderContext(DatabaseConfig config, String publicationName) {
        this.config = config;
        this.publicationName = publicationName;
    }

    public DatabaseConfig getConfig() {
        return config;
    }

    public String getPublicationName() {
        return publicationName;
    }
}
