/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.event.RemoveDatabaseSyncCacheEvent;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.DatabaseSyncProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-29 20:21
 */
@Component
public final class DatabaseSyncProfileImpl extends AbstractConfigModelProfile<DatabaseSyncTask> implements DatabaseSyncProfile, ApplicationListener<RemoveDatabaseSyncCacheEvent> {

    @Resource
    private TaskProfile taskProfile;

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
    public void onApplicationEvent(RemoveDatabaseSyncCacheEvent event) {
        removeCache(event.getCommonMessage().getId());
    }
}
