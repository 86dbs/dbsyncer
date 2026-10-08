/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.TaskDetailMetaProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-30 17:31
 */
@Component
public final class TaskDetailMetaProfileImpl implements TaskDetailMetaProfile {

    @Resource
    private StorageService storageService;

    @Resource
    private OperationTemplate operationTemplate;

    @Override
    public Meta getMeta(String tableGroupId) {
        return operationTemplate.queryObject(Meta.class, tableGroupId);
    }

    @Override
    public String add(Meta meta) {
        return "";
    }

    @Override
    public String update(Meta meta) {
        return "";
    }

    @Override
    public void remove(String tableGroupId) {

    }

    @Override
    public void reset(String tableGroupId) {
        Meta meta = getMeta(tableGroupId);
        if (meta != null) {
            meta.clear();
            meta.setUpdateTime(System.currentTimeMillis());
            operationTemplate.execute(meta, CommandEnum.OPR_EDIT);
        }
    }

    @Override
    public void clearData(String taskId) {
        storageService.clear(StorageEnum.TASK_DETAIL, taskId);
    }

    @Override
    public void dropTaskDetailTable(String taskId) {
        storageService.drop(StorageEnum.TASK_DETAIL, taskId);
    }

    @Override
    public void deleteMetaByTableGroupIds(List<String> tableGroupIds) {
        if (CollectionUtils.isEmpty(tableGroupIds)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.META);
        query.addFilter(ConfigConstant.META_IS_TASK_DETAIL, TaskLevelEnum.TASK_DETAIL.getCode());
        query.addFilter(ConfigConstant.META_TASK_ID, FilterEnum.IN, StringUtil.join(tableGroupIds, StringUtil.COMMA));
        storageService.delete(query);
    }
}
