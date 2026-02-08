/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter.impl;

import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.FilterTypeEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;

import java.util.Objects;

public class LongFilter extends AbstractFilter {

    public LongFilter(String name, FilterEnum filterEnum, long value) {
        setName(name);
        setFilter(filterEnum.getName());
        setValue(Objects.toString(value));
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.LONG;
    }
}
