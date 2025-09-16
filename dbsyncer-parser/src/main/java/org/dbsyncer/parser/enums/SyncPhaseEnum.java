package org.dbsyncer.parser.enums;

/**
 * 混合模式阶段枚举
 *
 * @author AE86
 * @version 1.0.0
 */
public enum SyncPhaseEnum {
    /**
     * 全量同步阶段
     */
    FULL(0, "全量同步阶段"),
    /**
     * 增量同步阶段
     */
    INCREMENTAL(1, "增量同步阶段");

    private final int code;
    private final String desc;

    SyncPhaseEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}