/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.enums;

/**
 * Content-Type类型
 *
 * @author 穿云
 * @version 1.0.0
 */
public enum ContentTypeEnum {

    /**
     * JSON
     */
    JSON("1","application/json;charset=UTF-8", "JSON格式"),

    /**
     * 表单
     */
    FORM("2","application/x-www-form-urlencoded;charset=UTF-8", "表单格式");

    private final String type;
    private final String code;
    private final String message;

    ContentTypeEnum(String type, String code, String message) {
        this.type = type;
        this.code = code;
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
