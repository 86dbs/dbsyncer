/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.List;
import java.util.function.Consumer;

/**
 * 订正校验任务
 *
 * @author wuji
 * @version 1.0.0
 */
public interface ValidateSyncProfile {

    /**
     * 按 id 查询 Mapping 任务配置。
     */
    ValidateSyncTask get(String id);

    /**
     * 新增任务配置。
     */
    String add(ValidateSyncTask task);

    /**
     * 修改任务配置。
     */
    String update(ValidateSyncTask tasks);

    /**
     * 删除任务配置
     */
    void delete(String id);

    /**
     * 按模型类型分页查询任务，可选按名称模糊搜索。
     *
     * @param searchKey 可选；非空时对 {@code name} 做 LIKE
     */
    Paging<ValidateSyncTask> query(int pageNum, int pageSize, String searchKey);

    /**
     * 按模型类型分页回调遍历全部任务。
     */
    void pageScanTasks(int pageSize, Consumer<List<ValidateSyncTask>> pageConsumer);

    /**
     * 预建运行明细分表
     */
    void createRunDetailTable(String taskId);

    /**
     * 保存/编辑时重建明细分表骨架行（先清空再按当前表映射写入）。
     */
    void syncTaskTableMetaDetails(String taskId);

}
