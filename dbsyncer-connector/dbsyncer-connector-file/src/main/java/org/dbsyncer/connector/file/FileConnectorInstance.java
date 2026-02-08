/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file;

import org.dbsyncer.connector.file.config.FileConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnectorInstance implements ConnectorInstance<FileConfig, String> {

    private FileConfig config;

    public FileConnectorInstance(FileConfig config) {
        this.config = config;
    }

    @Override
    public String getServiceUrl() {
        return config.getFileDir();
    }

    public FileConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(FileConfig config) {
        this.config = config;
    }

    @Override
    public String getConnection() {
        return config.getFileDir();
    }

    @Override
    public void close() {
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
