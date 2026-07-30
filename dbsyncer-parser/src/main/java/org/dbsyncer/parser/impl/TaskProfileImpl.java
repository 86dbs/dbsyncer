/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link TaskProfile} 实现（任务运行结果清理/重置）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class TaskProfileImpl implements TaskProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private StorageService storageService;

    @Override
    public void removeDetailMetasByTaskId(String taskId) {
        metaProfile.deleteMetaByTableGroupIds(tableGroupProfile.listTableGroupIds(taskId));
    }

    @Override
    public void clearTaskRunResults(String taskId) {

        if (StringUtil.isBlank(taskId)) {
            return;
        }
        List<String> groupIds = tableGroupProfile.listTableGroupIds(taskId);
        metaProfile.deleteMetaByTableGroupIds(groupIds);
        storageService.clear(StorageEnum.TASK_DETAIL, taskId);
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }
        // 表映射仍保留时补回空明细 Meta，供续跑/重跑使用
        List<Meta> metas = new ArrayList<>(groupIds.size());
        long now = System.currentTimeMillis();
        for (String groupId : groupIds) {
            Meta meta = new Meta();
            meta.setTaskId(groupId);
            meta.setIsTaskDetail(TaskLevelEnum.TASK_DETAIL.getCode());
            meta.setCreateTime(now);
            meta.setUpdateTime(now);
            metas.add(meta);
        }
        TaskSplitUtil.split(metas, ConfigConstant.PAGE_SIZE, (models) -> {
            operationTemplate.executeBatch(models, CommandEnum.OPR_ADD);
        });
    }

    @Override
    public void resetTaskMeta(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return;
        }
        Meta meta = metaProfile.getMetaByTaskId(taskId, TaskLevelEnum.TASK);
        if (meta == null) {
            return;
        }
        // success/fail/diff/fixed 靠 increment 维护，edit 时 preserveMetaCounters 会保留库值，须先增量归零
        zeroTaskMetaCounters(meta);
        meta.clear();
        meta.setTaskId(taskId);
        meta.setIsTaskDetail(TaskLevelEnum.TASK.getCode());
        meta.setUpdateTime(System.currentTimeMillis());
        operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_EDIT));
    }

    /**
     * 将任务级 Meta 计数原子归零（走 increment，避免 edit 被 preserve 覆盖）。
     */
    private void zeroTaskMetaCounters(Meta meta) {
        long total = counterValue(meta.getTotal());
        long success = counterValue(meta.getSuccess());
        long fail = counterValue(meta.getFail());
        long diff = counterValue(meta.getDiff());
        long fixed = counterValue(meta.getFixed());
        if (total == 0L && success == 0L && fail == 0L && diff == 0L && fixed == 0L) {
            return;
        }
        metaProfile.incrementMeta(MetaIncrement.of(meta.getId())
                .total(-total)
                .success(-success)
                .fail(-fail)
                .diff(-diff)
                .fixed(-fixed));
    }

    private static long counterValue(AtomicLong value) {
        return value == null ? 0L : value.get();
    }
}
