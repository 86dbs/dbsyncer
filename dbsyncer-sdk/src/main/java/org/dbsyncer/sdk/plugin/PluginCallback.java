/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import java.util.List;

/**
 * @Author wuJi
 * @Version 1.0.0
 * @Date 2026-04-03 14:54
 */
public interface PluginCallback {

    /**
     * 数据处理完成回调
     *
     * @param mappingId
     * @param tableGroupId
     * @param event
     * @param data
     */
    void onDataProcessed(String mappingId, String tableGroupId, String event, List<Object> data);

}
