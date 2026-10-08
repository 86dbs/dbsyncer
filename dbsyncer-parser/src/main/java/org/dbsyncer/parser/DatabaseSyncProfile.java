/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.DatabaseSyncTask;

import java.util.List;
import java.util.function.Consumer;

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
     * 修改任务配置。
     */
    String update(DatabaseSyncTask tasks);

    /**
     * 删除任务配置
     */
    void delete(String id);

    void clearRunData(String id);

    /**
     * 按模型类型分页查询任务，可选按名称模糊搜索。
     *
     * @param searchKey 可选；非空时对 {@code name} 做 LIKE
     */
    Paging<DatabaseSyncTask> query(int pageNum, int pageSize, String searchKey);

    /**
     * 按模型类型分页回调遍历全部任务。
     */
    void pageScanTasks(int pageSize, Consumer<List<DatabaseSyncTask>> pageConsumer);

    /**
     * 预建运行明细分表
     */
    void syncTaskTableMetaDetails(String taskId);

}
