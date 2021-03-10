package org.dbsyncer.storage.enums;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/3/10 20:31
 */
public enum StorageDataStatusEnum {

    /**
     * 失败
     */
    FAIL(0, "0", "失败"),
    /**
     * 成功
     */
    SUCCESS(1, "1", "成功");

    private Integer value;

    private String code;

    private String message;

    StorageDataStatusEnum(Integer value, String code, String message) {
        this.value = value;
        this.code = code;
        this.message = message;
    }

    public Integer getValue() {
        return value;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}