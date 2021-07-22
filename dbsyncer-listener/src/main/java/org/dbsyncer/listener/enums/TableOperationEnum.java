package org.dbsyncer.listener.enums;

public enum TableOperationEnum {

    /**
     * 插入
     */
    INSERT(2),
    /**
     * 更新（旧值）
     */
    UPDATE_BEFORE(3),
    /**
     * 更新（新值）
     */
    UPDATE_AFTER(4),
    /**
     * 删除
     */
    DELETE(1);

    private final int code;

    TableOperationEnum(int code) {
        this.code = code;
    }

    public static boolean isInsert(int code) {
        return INSERT.getCode() == code;
    }

    public static boolean isUpdateBefore(int code) {
        return UPDATE_BEFORE.getCode() == code;
    }

    public static boolean isUpdateAfter(int code) {
        return UPDATE_AFTER.getCode() == code;
    }

    public static boolean isDelete(int code) {
        return DELETE.getCode() == code;
    }

    public int getCode() {
        return code;
    }

}
