/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.List;

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
     * 批量新增任务配置。
     */
    void addBatch(List<ValidateSyncTask> task);

    /**
     * 修改任务配置。
     */
    String update(ValidateSyncTask tasks);

    /**
     * 删除任务配置
     */
    void delete(String id);

}
