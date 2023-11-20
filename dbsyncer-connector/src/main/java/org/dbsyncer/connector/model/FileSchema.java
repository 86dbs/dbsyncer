package org.dbsyncer.connector.model;

import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 0:04
 */
public class FileSchema {

    /**
     * 文件名
     */
    private String fileName;
    /**
     * 字段信息
     */
    private List<Field> fields;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
