package org.dbsyncer.parser.enums;



public enum CommonTaskStatusEnum {

    READY(0, "未运行"),
    RUNNING(1, "运行中"),
    SUCCESS(2, "执行成功"),
    FAIL(3, "执行失败");

    private final int code;
    private final String message;

    CommonTaskStatusEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static CommonTaskStatusEnum getByCode(int code) {
        for (CommonTaskStatusEnum e : values()) {
            if (e.getCode() == code) {
                return e;
            }
        }
        return null;
    }
}
