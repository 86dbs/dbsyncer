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
    JSON("1", "application/json", "JSON格式"),

    /**
     * 表单
     */
    FORM_URLENCODED("2", "application/x-www-form-urlencoded", "表单格式");

    private final String type;
    private final String code;
    private final String message;

    ContentTypeEnum(String type, String code, String message) {
        this.type = type;
        this.code = code;
        this.message = message;
    }

    /**
     * 根据 value 获取枚举
     *
     * @param type Content-Type 值
     * @return 枚举对象
     */
    public static ContentTypeEnum fromValue(String type) {
        if (type == null) {
            return null;
        }
        for (ContentTypeEnum contentType : values()) {
            if (contentType.getType().equalsIgnoreCase(type)) {
                return contentType;
            }
        }
        return null;
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
