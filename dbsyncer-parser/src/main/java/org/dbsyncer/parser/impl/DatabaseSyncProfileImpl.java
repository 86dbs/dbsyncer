/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.event.RemoveDatabaseSyncCacheEvent;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.DatabaseSyncProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.DatabaseSyncDetailTypeEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.TaskDetailUtil;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-29 20:21
 */
@Component
public final class DatabaseSyncProfileImpl extends AbstractConfigModelProfile<DatabaseSyncTask> implements DatabaseSyncProfile, ApplicationListener<RemoveDatabaseSyncCacheEvent> {

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Override
    public DatabaseSyncTask get(String id) {
        return getCache(id);
    }

    @Override
    public String add(DatabaseSyncTask task) {
        return taskProfile.addTask(task);
    }

    @Override
    public void addBatch(List<DatabaseSyncTask> tasks) {
        taskProfile.addTaskBatch(tasks);
    }

    @Override
    public String update(DatabaseSyncTask task) {
        String id = taskProfile.updateTask(task);
        removeCacheAndNotice(task.getId());
        return id;
    }

    @Override
    public void delete(String id) {
        taskProfile.deleteTask(id);
        removeCacheAndNotice(id);
    }

    @Override
    public void clearRunData(String id) {
        taskProfile.clearRunData(id);
    }

    @Override
    public Paging<DatabaseSyncTask> query(int pageNum, int pageSize, String searchKey) {
        return taskProfile.queryTasks(DatabaseSyncTask.class, pageNum, pageSize, searchKey);
    }

    @Override
    public void pageScanTasks(int pageSize, Consumer<List<DatabaseSyncTask>> pageConsumer) {
        taskProfile.pageScanTasks(DatabaseSyncTask.class, pageSize, pageConsumer);
    }

    @Override
    public void createRunDetailTable(String taskId) {
        taskProfile.createRunDetailTable(taskId);
    }

    @Override
    public void onApplicationEvent(RemoveDatabaseSyncCacheEvent event) {
        removeCache(event.getCommonMessage().getId());
        metaProfile.removeMetaCache(event.getCommonMessage().getId());
    }

    /**
     * 保存/编辑时重建明细分表骨架行：先清空分表，再按当前表映射 × 开启类型全量插入 READY 行。
     */
    public void syncTaskTableMetaDetails(String taskId) {
        Assert.hasText(taskId, "任务ID不能为空");
        DatabaseSyncTask task = get(taskId);
        Assert.notNull(task, "任务不存在");
        storageService.clear(StorageEnum.TASK_DETAIL, taskId);
        List<String> types = resolveEnabledDetailTypes(task);
        if (types.isEmpty()) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        tableGroupProfile.pageScanTableGroups(taskId, ConfigConstant.PAGE_SIZE, page -> {
            if (CollectionUtils.isEmpty(page)) {
                return;
            }
            List<Map> toAdd = new ArrayList<>();
            for (TableGroup group : page) {
                if (group == null || StringUtil.isBlank(group.getId())) {
                    continue;
                }
                String targetTable = group.getTargetTable() == null ? StringUtil.EMPTY : group.getTargetTable().getName();
                for (String type : types) {
                    toAdd.add(newReadyRow(group.getId(), type, targetTable, now));
                }
            }
            if (!CollectionUtils.isEmpty(toAdd)) {
                storageService.addBatch(StorageEnum.TASK_DETAIL, taskId, toAdd);
            }
        });
    }

    private List<String> resolveEnabledDetailTypes(DatabaseSyncTask task) {
        List<String> types = new ArrayList<>(2);
        if (task.isEnableCopySchema()) {
            types.add(DatabaseSyncDetailTypeEnum.TABLE_SCHEMA.getCode());
        }
        if (task.isEnableCopyData()) {
            types.add(DatabaseSyncDetailTypeEnum.ROW_DATA.getCode());
        }
        return types;
    }

    private Map<String, Object> newReadyRow(String tableGroupId, String type, String targetTable, long now) {
        Map<String, Object> content = new HashMap<>(2);
        content.put(ConfigConstant.TASK_STATUS, CommonTaskStatusEnum.READY.getCode());
        Map<String, Object> row = new HashMap<>();
        row.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
        row.put(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
        row.put(ConfigConstant.CONFIG_MODEL_TYPE, type);
        row.put(ConfigConstant.DETAIL_TARGET_TABLE, StringUtil.getIfBlank(targetTable, StringUtil.EMPTY));
        // 成败字段默认失败位；生命周期在 DATA.status / meta.STATE
        row.put(ConfigConstant.DETAIL_IS_SUCCESS, StorageDataStatusEnum.FAIL.getValue());
        row.put(ConfigConstant.BINLOG_DATA, TaskDetailUtil.serializeContent(content));
        row.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
        row.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, now);
        return row;
    }
}
