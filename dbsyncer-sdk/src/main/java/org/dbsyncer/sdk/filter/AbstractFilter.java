/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter;

import org.dbsyncer.sdk.enums.FilterTypeEnum;
import org.dbsyncer.sdk.model.Filter;

/**
 * 过滤语法实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-17 23:56
 */
public abstract class AbstractFilter extends Filter {

    /**
     * 返回是否显示高亮
     */
    private boolean enableHighLightSearch;

    /**
     * 参数类型
     *
     * @return
     */
    public abstract FilterTypeEnum getFilterTypeEnum();

    public boolean isEnableHighLightSearch() {
        return enableHighLightSearch;
    }

    public void setEnableHighLightSearch(boolean enableHighLightSearch) {
        this.enableHighLightSearch = enableHighLightSearch;
    }
}
