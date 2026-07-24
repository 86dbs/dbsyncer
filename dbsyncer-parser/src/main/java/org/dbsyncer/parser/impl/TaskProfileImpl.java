/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

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

    @Override
    public void removeDetailMetasByTaskId(String taskId) {
        operationTemplate.removeDetailMetasByTableGroupIds(operationTemplate.listTableGroupIds(taskId));
    }

    @Override
    public void clearTaskRunResults(String taskId) {
        operationTemplate.clearTaskRunResults(taskId);
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
        meta.clear();
        meta.setTaskId(taskId);
        meta.setIsTaskDetail(0);
        meta.setUpdateTime(System.currentTimeMillis());
        operationTemplate.execute(new OperationConfig(meta, CommandEnum.OPR_EDIT));
    }
}
