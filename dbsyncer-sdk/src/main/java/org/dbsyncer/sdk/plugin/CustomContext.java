/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.filter.BooleanFilter;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-04-22 13:47
 */
public interface CustomContext {

    /**
     * 是否是目标连接器
     */
    boolean isTargetConnector();

    /**
     * 获取筛选条件
     */
    BooleanFilter getFilter();

}