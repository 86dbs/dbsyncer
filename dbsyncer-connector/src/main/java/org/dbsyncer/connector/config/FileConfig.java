package org.dbsyncer.connector.config;

import org.dbsyncer.connector.model.FileSchema;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public class FileConfig extends ConnectorConfig {

    /**
     * 文件目录
     */
    private String fileDir;

    /**
     * 文件描述信息
     */
    private List<FileSchema> fileSchema;

    public String getFileDir() {
        return fileDir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    public List<FileSchema> getFileSchema() {
        return fileSchema;
    }

    public void setFileSchema(List<FileSchema> fileSchema) {
        this.fileSchema = fileSchema;
    }
}