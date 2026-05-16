/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.CommonTask;

import java.util.List;
import java.util.Map;

/**
 * 任务调度服务
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-05-12 23:36
 */
public interface TaskService<T extends CommonTask> {

    /**
     * 新增
     */
    String add(T task);

    /**
     * 修改
     */
    String edit(T task);

    /**
     * 删除
     */
    void delete(String id);

    /**
     * 启动
     */
    void start(String id);

    /**
     * 停止
     */
    void stop(String id);

    /**
     * 获取任务
     */
    T get(String id);

    /**
     * 任务列表
     */
    Paging search(Map<String, String> param, CommonTaskTypeEnum commonTaskTypeEnum);

    /**
     * 查看任务执行详情
     */
    Paging result(String id);

    /**
     * 获取所有任务 根据任务类型
     *
     * @return
     */
    List<CommonTask> getTaskAll(CommonTaskTypeEnum commonTaskTypeEnum);

    /**
     * 检查任务状态
     *
     * @param taskId
     * @return
     */
    boolean isRunning(String taskId);

}
