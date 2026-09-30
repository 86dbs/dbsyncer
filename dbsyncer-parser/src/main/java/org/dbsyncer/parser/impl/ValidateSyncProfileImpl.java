/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.event.RemoveValidateSyncCacheEvent;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.ValidateSyncProfile;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-29 20:21
 */
@Component
public final class ValidateSyncProfileImpl extends AbstractConfigModelProfile<ValidateSyncTask> implements ValidateSyncProfile, ApplicationListener<RemoveValidateSyncCacheEvent> {

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Override
    public ValidateSyncTask get(String id) {
        return getCache(id);
    }

    @Override
    public String add(ValidateSyncTask task) {
        return taskProfile.addTask(task);
    }

    @Override
    public void addBatch(List<ValidateSyncTask> tasks) {
        taskProfile.addTaskBatch(tasks);
    }

    @Override
    public String update(ValidateSyncTask task) {
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
    public Paging<ValidateSyncTask> query(int pageNum, int pageSize, String searchKey) {
        return taskProfile.queryTasks(ValidateSyncTask.class, pageNum, pageSize, searchKey);
    }

    @Override
    public void pageScanTasks(int pageSize, Consumer<List<ValidateSyncTask>> pageConsumer) {
        taskProfile.pageScanTasks(ValidateSyncTask.class, pageSize, pageConsumer);
    }

    @Override
    public void createRunDetailTable(String taskId) {
        taskProfile.createRunDetailTable(taskId);
    }

    @Override
    public void onApplicationEvent(RemoveValidateSyncCacheEvent event) {
        removeCache(event.getCommonMessage().getId());
        metaProfile.removeMetaCache(event.getCommonMessage().getId());
    }
}
