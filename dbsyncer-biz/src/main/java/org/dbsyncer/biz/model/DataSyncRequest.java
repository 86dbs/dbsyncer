/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

import java.util.List;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-23 15:24
 */
public class DataSyncRequest {

    /**
     * 驱动任务id
     */
    private String mappingId;

    /**
     * 表任务id
     */
    private String tableGroupId;

    /**
     * 变更数据
     */
    private List<DataSyncEvent> dataList;

    public String getMappingId() {
        return mappingId;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public List<DataSyncEvent> getDataList() {
        return dataList;
    }

    public void setDataList(List<DataSyncEvent> dataList) {
        this.dataList = dataList;
    }
}
