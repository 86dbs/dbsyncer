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
     * 游标
     */
    CURSOR("cursor", 0),

    /**
     * 页数
     */
    PAGE_INDEX("pageIndex", 1),

    /**
     * 执行的表映射关系索引
     */
    TABLE_GROUP_INDEX("tableGroupIndex", 0);

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