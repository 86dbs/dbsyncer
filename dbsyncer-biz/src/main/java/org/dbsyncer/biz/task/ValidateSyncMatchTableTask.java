/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.common.dispatch.AbstractDispatchTask;
import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 订正校验任务异步匹配相似表
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/4/8
 */
public class ValidateSyncMatchTableTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String taskId;

    private TaskService<ValidateSyncTask> taskService;

    private ValidateSyncService validateSyncService;

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.VALIDATE_SYNC_MATCH_TABLE;
    }

    @Override
    public String getUniqueId() {
        return taskId;
    }

    @Override
    public void execute() {
        ValidateSyncTask task = taskService.get(taskId);
        if (task == null) {
            logger.warn("ValidateSyncTask not found, skip match table, taskId={}", taskId);
            return;
        }
        matchSimilarTableGroups(task);
    }

    private void matchSimilarTableGroups(ValidateSyncTask validateSyncTask) {
        List<Table> sourceTables = validateSyncTask.getSourceTable();
        List<Table> targetTables = validateSyncTask.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            logger.warn("源库或目标库表列表为空，跳过匹配相似表, taskId={}", taskId);
            return;
        }
        Map<String, Table> targetTableMap = new LinkedHashMap<>();
        for (Table table : targetTables) {
            if (table == null || StringUtil.isBlank(table.getName())) {
                continue;
            }
            targetTableMap.putIfAbsent(table.getName().toUpperCase(Locale.ROOT), table);
        }

        for (Table sourceTable : sourceTables) {
            if (sourceTable == null || StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            Table targetTable = targetTableMap.get(sourceTable.getName().toUpperCase(Locale.ROOT));
            if (targetTable == null) {
                continue;
            }
            String targetType = targetTable.getType();
            if (StringUtil.isNotBlank(targetType) && !TableTypeEnum.isTable(targetType)) {
                continue;
            }
            addMatchedTableGroup(validateSyncTask.getId(), sourceTable, targetTable);
        }
    }

    private void addMatchedTableGroup(String taskId, Table sourceTable, Table targetTable) {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("taskId", taskId);
            params.put("sourceTable", sourceTable.getName());
            params.put("targetTable", targetTable.getName());
            params.put("sourceType", StringUtil.isNotBlank(sourceTable.getType()) ? sourceTable.getType() : TableTypeEnum.TABLE.getCode());
            params.put("targetType", StringUtil.isNotBlank(targetTable.getType()) ? targetTable.getType() : TableTypeEnum.TABLE.getCode());
            validateSyncService.addTableGroup(params);
        } catch (Exception e) {
            logger.error("添加表映射失败: {} >> {}, {}", sourceTable.getName(), targetTable.getName(), e.getMessage());
        }
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setTaskService(TaskService<ValidateSyncTask> taskService) {
        this.taskService = taskService;
    }

    public void setValidateSyncService(ValidateSyncService validateSyncService) {
        this.validateSyncService = validateSyncService;
    }
}
