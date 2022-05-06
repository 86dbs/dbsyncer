package org.dbsyncer.connector.config;

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
     * 分隔符
     */
    private String separator;

    /**
     * 文件描述信息
     */
    private String schema;

    public String getFileDir() {
        return fileDir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}