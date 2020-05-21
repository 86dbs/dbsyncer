package org.dbsyncer.parser.enums;

/**
 * 错误日志枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public enum ErrorEnum {

    SYSTEM("0", "系统日志"),
    CONNECT_FAILED("1", "连接器连接失败"),
    RUNNING("2", "启动驱动"),
    STOPPING("3", "停止驱动");

    private String type;
    private String message;

    ErrorEnum(String type, String message) {
        this.type = type;
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

}