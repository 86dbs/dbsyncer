/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.config;

import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class FileConfig extends ConnectorConfig {

    /**
     * 文件目录
     */
    private String fileDir;

    public String getFileDir() {
        return fileDir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    public String getUrl() {
        return fileDir;
    }
}
