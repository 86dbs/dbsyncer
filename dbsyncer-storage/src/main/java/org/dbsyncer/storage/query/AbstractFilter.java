/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.query;

import org.apache.lucene.search.Query;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.model.Filter;

/**
 * 过滤语法实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-17 23:56
 */
public abstract class AbstractFilter extends Filter {
    private boolean enableHighLightSearch;

    public AbstractFilter(String name, FilterEnum filterEnum, Object value) {
        this(name, filterEnum, value, false);
    }

    public AbstractFilter(String name, FilterEnum filterEnum, Object value, boolean enableHighLightSearch) {
        setName(name);
        setFilter(filterEnum.getName());
        setValue(String.valueOf(value));
        this.enableHighLightSearch = enableHighLightSearch;
    }

    public abstract Query newEqual();

    public abstract Query newLessThan();

    public boolean isEnableHighLightSearch() {
        return enableHighLightSearch;
    }
}