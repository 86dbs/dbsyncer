/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.parser.TaskDetailMetaProfile;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

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
    public Meta getMeta(String taskId) {
        return null;
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
    public void remove(String taskId) {

    }

    @Override
    public void reset(String taskId) {

    }

    @Override
    public void clearData(String id) {
        storageService.clear(StorageEnum.TASK_DETAIL, id);
    }
}
