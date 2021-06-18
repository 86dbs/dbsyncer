package org.dbsyncer.listener.sqlserver;

public enum TableOperation {
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

    TableOperation(int code) {
        this.code = code;
    }

    public static boolean isInsert(int code) {
        return INSERT.getCode() == code;
    }

    public static boolean isUpdate(int code) {
        return UPDATE_AFTER.getCode() == code;
    }

    public static boolean isDelete(int code) {
        return DELETE.getCode() == code;
    }

    public int getCode() {
        return code;
    }
}
