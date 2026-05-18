/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter.impl;

import org.dbsyncer.sdk.enums.FilterTypeEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 运行时 JDBC {@code IN} 列表：生成 {@code col IN (?,?,?)}，并按顺序绑定 {@link #getBindValues()}。
 * @author wuji
 */
public class InFilter extends AbstractFilter {

    private final List<Object> bindValues;

    /**
     * @param columnName 列名（与表字段一致；连接器内会做下划线/引号处理）
     * @param bindValues 与 {@code IN} 中 {@code ?} 一一对应的绑定值（可为 null 表示 SQL NULL）
     */
    public InFilter(String columnName, List<Object> bindValues) {
        setName(columnName);
        this.bindValues = bindValues == null ? Collections.emptyList() : new ArrayList<>(bindValues);
    }

    public List<Object> getBindValues() {
        return bindValues;
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.STRING;
    }
}
