/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.common.dispatch.AbstractDispatchTask;
import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ValidateSyncProfile;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
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
@Service
public final class ValidateSyncMatchTableTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ValidateSyncService validateSyncService;

    @Resource
    private ValidateSyncProfile validateSyncProfile;

    private String taskId;

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
        ValidateSyncTask task = validateSyncProfile.get(taskId);
        if (task == null) {
            logger.warn("ValidateSyncTask not found, skip match table, taskId={}", taskId);
            return;
        }
        matchSimilarTableGroups(task);
    }

    /**
     * 批量写入相似表映射，整批结束后由 addTableGroupBatch 只对齐一次明细。
     */
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

        List<String> sourceNames = new ArrayList<>();
        List<String> targetNames = new ArrayList<>();
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
            sourceNames.add(sourceTable.getName());
            targetNames.add(targetTable.getName());
        }
        if (sourceNames.isEmpty()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("taskId", validateSyncTask.getId());
        params.put("sourceTable", StringUtil.join(sourceNames, StringUtil.VERTICAL_LINE));
        params.put("targetTable", StringUtil.join(targetNames, StringUtil.VERTICAL_LINE));
        params.put("sourceType", TableTypeEnum.TABLE.getCode());
        params.put("targetType", TableTypeEnum.TABLE.getCode());
        validateSyncService.addTableGroupBatch(params);
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

}
