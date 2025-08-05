/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.filter.CompareFilter;

import java.util.Arrays;

/**
 * 运算符表达式类型
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-09-26 23:21
 */
public enum FilterEnum {

    /**
     * 等于
     */
    EQUAL("=", StringUtil::equals),
    /**
     * 不等于
     */
    NOT_EQUAL("!=", (value, filterValue) -> !StringUtil.equals(value, filterValue)),
    /**
     * 大于
     */
    GT(">", (value, filterValue) -> NumberUtil.toLong(value) > NumberUtil.toLong(filterValue)),
    /**
     * 小于
     */
    LT("<", (value, filterValue) -> NumberUtil.toLong(value) < NumberUtil.toLong(filterValue)),
    /**
     * 大于等于
     */
    GT_AND_EQUAL(">=", (value, filterValue) -> NumberUtil.toLong(value) >= NumberUtil.toLong(filterValue)),
    /**
     * 小于等于
     */
    LT_AND_EQUAL("<=", (value, filterValue) -> NumberUtil.toLong(value) <= NumberUtil.toLong(filterValue)),
    /**
     * 模糊匹配
     */
    LIKE("like", (value, filterValue) -> {
        boolean startsWith = StringUtil.startsWith(filterValue, StringUtil.PERCENT) || StringUtil.startsWith(filterValue, StringUtil.STAR);
        boolean endsWith = StringUtil.endsWith(filterValue, StringUtil.PERCENT) || StringUtil.endsWith(filterValue, StringUtil.STAR);
        String compareValue = StringUtil.replace(filterValue, StringUtil.PERCENT, StringUtil.EMPTY);
        compareValue = StringUtil.replace(compareValue, StringUtil.STAR, StringUtil.EMPTY);
        // 模糊匹配
        if (startsWith && endsWith) {
            return StringUtil.contains(value, compareValue);
        }
        // 前缀匹配
        if (endsWith) {
            return StringUtil.startsWith(value, compareValue);
        }
        // 后缀匹配
        if (startsWith) {
            return StringUtil.endsWith(value, compareValue);
        }
        return false;
    }),
    /**
     * 集合匹配
     */
    IN("in", (value, filterValue) -> {
        if (StringUtil.isNotBlank(filterValue)) {
            return Arrays.stream(StringUtil.split(filterValue, StringUtil.COMMA)).anyMatch(v -> StringUtil.equals(v, value));
        }
        return false;
    }),
    /**
     * Null值
     */
    IS_NULL("is null", (value, filterValue) -> value == null),
    /**
     * 非Null值
     */
    IS_NOT_NULL("is not null", (value, filterValue) -> value != null);

    // 运算符名称
    private final String name;
    // 比较器
    private final CompareFilter compareFilter;

    FilterEnum(String name, CompareFilter compareFilter) {
        this.name = name;
        this.compareFilter = compareFilter;
    }

    /**
     * 获取表达式
     *
     * @param name
     * @return
     * @throws SdkException
     */
    public static FilterEnum getFilterEnum(String name) throws SdkException {
        for (FilterEnum e : FilterEnum.values()) {
            if (StringUtil.equals(name, e.getName())) {
                return e;
            }
        }
        throw new SdkException(String.format("FilterEnum name \"%s\" does not exist.", name));
    }

    /**
     * 获取比较器
     *
     * @param filterName
     * @return
     * @throws SdkException
     */
    public static CompareFilter getCompareFilter(String filterName) throws SdkException {
        for (FilterEnum e : FilterEnum.values()) {
            if (StringUtil.equals(filterName, e.getName())) {
                return e.getCompareFilter();
            }
        }
        throw new SdkException(String.format("FilterEnum name \"%s\" does not exist.", filterName));
    }

    public String getName() {
        return name;
    }

    public CompareFilter getCompareFilter() {
        return compareFilter;
    }

}