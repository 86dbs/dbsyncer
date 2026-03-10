/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.CommonTask;

import java.util.Map;

/**
 * 任务调度服务
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-05-12 23:36
 */
public interface TaskService {

    /**
     * 新增任务
     */
    String add(Map<String, String> params);

    /**
     * 修改
     */
    String edit(Map<String, String> params);

    /**
     * 删除任务
     */
    void delete(String id);

    /**
     * 启动
     */
    void start(String id);

    /**
     * 停止任务
     */
    void stop(String id);

    /**
     * 获取任务详情
     */
    CommonTask get(String id);

    /**
     * 任务列表
     */
    Paging search(Map<String, String> param);

    /**
     * 查看任务结果
     */
    Paging result(String id);
}
