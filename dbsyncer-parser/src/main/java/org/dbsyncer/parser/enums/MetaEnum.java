package org.dbsyncer.parser.enums;

/**
 * 任务级 Meta 运行态
 * 0-未运行；1-运行中；2-停止中；3-已完成。
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
    STOPPING(2, "停止中"),
    /**
     * 已完成
     */
    DONE(3, "已完成");

    private final int code;
    private final String message;

    MetaEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static boolean isRunning(int state) {
        return RUNNING.getCode() == state || STOPPING.getCode() == state;
    }

    public static boolean isDone(int state) {
        return DONE.getCode() == state;
    }

    public static boolean isDone(Integer state) {
        return state != null && isDone(state.intValue());
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
