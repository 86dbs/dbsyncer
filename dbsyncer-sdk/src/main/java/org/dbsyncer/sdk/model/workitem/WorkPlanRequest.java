/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.workitem;

import org.dbsyncer.sdk.model.Table;

import java.util.Collections;
import java.util.Map;

/**
 * 工作项规划请求参数。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-25
 */
public final class WorkPlanRequest {

    private final String tableGroupId;
    private final Table sourceTable;
    private final String schema;
    private final int pageSize;
    private final int onlineNodeCount;
    private final long taskBatchSize;
    private final Map<String, String> command;

    public WorkPlanRequest(String tableGroupId, Table sourceTable, String schema, int pageSize, int onlineNodeCount) {
        this(tableGroupId, sourceTable, schema, pageSize, onlineNodeCount, 0L, null);
    }

    public WorkPlanRequest(String tableGroupId, Table sourceTable, String schema, int pageSize,
                           int onlineNodeCount, long taskBatchSize) {
        this(tableGroupId, sourceTable, schema, pageSize, onlineNodeCount, taskBatchSize, null);
    }

    public WorkPlanRequest(String tableGroupId, Table sourceTable, String schema, int pageSize,
                           int onlineNodeCount, long taskBatchSize, Map<String, String> command) {
        this.tableGroupId = tableGroupId;
        this.sourceTable = sourceTable;
        this.schema = schema;
        this.pageSize = pageSize;
        this.onlineNodeCount = onlineNodeCount;
        this.taskBatchSize = taskBatchSize;
        this.command = command == null ? Collections.emptyMap() : command;
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public String getSchema() {
        return schema;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getOnlineNodeCount() {
        return onlineNodeCount;
    }

    /**
     * 显式任务批次大小；&lt;=0 时由公式按 pageSize/节点数推算。
     *
     * @return 行预算提示
     */
    public long getTaskBatchSize() {
        return taskBatchSize;
    }

    /**
     * 表组已生成的源端 command（含定位键 SQL）。
     *
     * @return command；可能为空 map
     */
    public Map<String, String> getCommand() {
        return command;
    }
}
