package org.dbsyncer.storage.query;

import org.apache.lucene.search.Query;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.sdk.model.Filter;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/17 23:56
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