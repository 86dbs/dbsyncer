package org.dbsyncer.connector.enums;

/**
 * 运算符表达式类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/26 23:21
 */
public enum FilterEnum {

    /**
     * 等于
     */
    EQUAL("="),
    /**
     * 不等于
     */
    NOT_EQUAL("!="),
    /**
     * 大于
     */
    GT(">"),
    /**
     * 小于
     */
    LT("<"),
    /**
     * 大于等于
     */
    GT_AND_EQUAL(">="),
    /**
     * 小于等于
     */
    LT_AND_EQUAL("<=");

    // 运算符名称
    private String name;

    FilterEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}