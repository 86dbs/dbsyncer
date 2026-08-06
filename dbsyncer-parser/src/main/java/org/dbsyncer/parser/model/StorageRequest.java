/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.parser.flush.BufferRequest;

import java.util.Map;

/**
 * 明细分表写入请求。
 * <p>{@code taskDetailShardId} 为任务 ID，对应 {@code dbsyncer_task_detail_{taskId}}；
 * {@link #getMetaId()} 为 BufferRequest 兼容别名，返回同一分片键。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class StorageRequest implements BufferRequest {

    /**
     * 明细分表分片键（任务 ID）
     */
    private final String taskDetailShardId;

    private final Map row;

    public StorageRequest(String taskDetailShardId, Map row) {
        this.taskDetailShardId = taskDetailShardId;
        this.row = row;
    }

    /**
     * BufferRequest 兼容：返回明细分表分片键（任务 ID），不是 Meta 主键。
     */
    @Override
    public String getMetaId() {
        return taskDetailShardId;
    }

    /**
     * 明细分表分片键（任务 ID）。
     */
    public String getTaskDetailShardId() {
        return taskDetailShardId;
    }

    public Map getRow() {
        return row;
    }
}
