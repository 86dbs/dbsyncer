package org.dbsyncer.parser.enums;

import org.dbsyncer.parser.ParserException;

/**
 * 驱动状态枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public enum MetaEnum {

    /**
     * 未运行
     */
    READY(0, "未运行"),
    /**
     * 运行中
     */
    RUNNING(1, "运行中"),
    /**
     * 停止中
     */
    STOPPING(2, "停止中");

    private int code;
    private String message;

    MetaEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static boolean isRunning(int state) {
        return RUNNING.getCode() == state || STOPPING.getCode() == state;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}