package org.dbsyncer.connector.file;

import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.FileConfig;

import java.io.File;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnectorMapper implements ConnectorMapper<FileConfig, File> {
    private FileConfig config;
    private File file;

    public FileConnectorMapper(FileConfig config) {
        this.config = config;
        file = new File(config.getFileDir());
    }

    public FileConfig getConfig() {
        return config;
    }

    @Override
    public File getConnection() {
        return file;
    }

    @Override
    public void close() {

    }
}
