package org.dbsyncer.connector.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;

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
    EQUAL("equal", "="),
    /**
     * 不等于
     */
    NOT_EQUAL("notEqual", "!="),
    /**
     * 大于
     */
    GT("gt", ">"),
    /**
     * 小于
     */
    LT("lt", "<"),
    /**
     * 大于等于
     */
    GT_AND_EQUAL("gtAndEqual", ">="),
    /**
     * 小于等于
     */
    LT_AND_EQUAL("ltAndEqual", "<=");

    // 运算符名称
    private String name;

    // 运算符
    private String code;

    FilterEnum(String name, String code) {
        this.name = name;
        this.code = code;
    }

    public static String getCode(String name){
        for (FilterEnum e : FilterEnum.values()) {
            if (StringUtils.equals(name, e.getName())) {
                return e.getCode();
            }
        }
        throw new ConnectorException(String.format("Filter name \"%s\" does not exist.", name));
    }

    public String getName() {
        return name;
    }

    public String getCode() {
        return code;
    }

}