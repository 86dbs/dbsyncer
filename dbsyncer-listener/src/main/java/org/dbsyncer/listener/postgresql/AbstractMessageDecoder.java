package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:04
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    protected DatabaseConfig config;

    @Override
    public String getSlotName() {
        return String.format("dbsyncer_%s_%s", config.getSchema(), config.getUsername());
    }

    @Override
    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }
}