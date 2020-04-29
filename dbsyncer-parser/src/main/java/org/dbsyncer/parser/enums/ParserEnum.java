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
     * 页数
     */
    PAGE_INDEX("pageIndex", "1");

    /**
     * 编码
     */
    private String code;
    /**
     * 默认值
     */
    private String defaultValue;

    ParserEnum(String code, String defaultValue) {
        this.code = code;
        this.defaultValue = defaultValue;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
}