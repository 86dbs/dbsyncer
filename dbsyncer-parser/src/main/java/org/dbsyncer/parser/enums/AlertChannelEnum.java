/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

public enum AlertChannelEnum {
    /**
     * 邮件
     */
    EMAIL(1, "邮件"),
    /**
     * HTTP
     */
    HTTP(2, "HTTP"),
    /**
     * 企业微信
     */
    WE_CHAT(3, "企业微信");

    private final int code;
    private final String message;

    AlertChannelEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
