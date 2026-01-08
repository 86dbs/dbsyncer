/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.model;

import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-06 00:04
 */
@Deprecated
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