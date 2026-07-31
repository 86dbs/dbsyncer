/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.flush.BufferResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 明细分表批量写入响应。
 * <p>{@code taskDetailShardId} 为任务 ID；{@link #getMetaId()}/{@link #setMetaId(String)} 为兼容别名。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class StorageResponse implements BufferResponse {

    /**
     * 明细分表分片键（任务 ID）
     */
    private String taskDetailShardId;
    private List<Map> dataList = new LinkedList<>();

    /**
     * BufferRequest 兼容：返回明细分表分片键（任务 ID）。
     */
    public String getMetaId() {
        return taskDetailShardId;
    }

    /**
     * BufferRequest 兼容：设置明细分表分片键（任务 ID）。
     */
    public void setMetaId(String taskDetailShardId) {
        this.taskDetailShardId = taskDetailShardId;
    }

    public String getTaskDetailShardId() {
        return taskDetailShardId;
    }

    public void setTaskDetailShardId(String taskDetailShardId) {
        this.taskDetailShardId = taskDetailShardId;
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public void setDataList(List<Map> dataList) {
        this.dataList = dataList;
    }

    @Override
    public int getTaskSize() {
        return dataList.size();
    }

    @Override
    public String getSuffixName() {
        return StringUtil.EMPTY;
    }
}
