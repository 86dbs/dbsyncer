package org.dbsyncer.parser.enums;

/**
 * 解析器参数枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/29 10:19
 */
public enum ParserEnum {

    /**
     * 游标（分片内页进度的历史 snapshot 键）。
     */
    CURSOR("cursor", 0),

    /**
     * 页码（分片内页进度的历史 snapshot 键）。
     */
    PAGE_INDEX("pageIndex", 1),

    /**
     * 表映射索引（历史 snapshot 键）。
     */
    TABLE_GROUP_INDEX("tableGroupIndex", 0),

    /**
     * 全量多表进度（JSON：tableGroupId -> {pageIndex,cursor,done}）
     */
    TABLE_PROGRESS("tableProgress", 0),

    /**
     * 表内 range 计划（JSON：tableGroupId -> [itemId...]）
     */
    TABLE_RANGE_PLAN("tableRangePlan", 0),

    /**
     * 全量+增量阶段: full(全量中) / increment(增量中)
     */
    FULL_INCREMENT_PHASE("fullIncrementPhase", 0);

    /**
     * 编码
     */
    private final String code;

    /**
     * 默认值
     */
    private final int defaultValue;

    ParserEnum(String code, int defaultValue) {
        this.code = code;
        this.defaultValue = defaultValue;
    }

    public String getCode() {
        return code;
    }

    public int getDefaultValue() {
        return defaultValue;
    }
}
