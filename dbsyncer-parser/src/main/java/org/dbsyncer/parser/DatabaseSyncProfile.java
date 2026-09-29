/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.sdk.model.DatabaseSyncTask;

import java.util.List;

/**
 * 整库迁移任务
 *
 * @author wuji
 * @version 1.0.0
 */
public interface DatabaseSyncProfile {

    /**
     * 按 id 查询 Mapping 任务配置。
     */
    DatabaseSyncTask get(String id);

    /**
     * 新增任务配置。
     */
    String add(DatabaseSyncTask task);

    /**
     * 批量新增任务配置。
     */
    void addBatch(List<DatabaseSyncTask> task);

    /**
     * 修改任务配置。
     */
    String update(DatabaseSyncTask tasks);

    /**
     * 删除任务配置
     */
    void delete(String id);

}
