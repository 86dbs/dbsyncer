/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter.impl;

import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.FilterTypeEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;

import java.util.Objects;

public class IntFilter extends AbstractFilter {

    public IntFilter(String name, int value) {
        setName(name);
        setFilter(FilterEnum.EQUAL.getName());
        setValue(Objects.toString(value));
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.INT;
    }
}
