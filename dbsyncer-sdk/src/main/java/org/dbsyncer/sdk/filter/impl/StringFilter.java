/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter.impl;

import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.FilterTypeEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;

public class StringFilter extends AbstractFilter {

    public StringFilter(String name, FilterEnum filterEnum, String value, boolean enableHighLightSearch) {
        setName(name);
        setFilter(filterEnum.getName());
        setValue(value);
        setEnableHighLightSearch(enableHighLightSearch);
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.STRING;
    }
}