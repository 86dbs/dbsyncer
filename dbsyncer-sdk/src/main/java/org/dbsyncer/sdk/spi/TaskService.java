/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.sdk.model.CommonTask;

import java.util.List;
import java.util.Map;

/**
 * 任务调度服务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-05-12 23:36
 */
public interface TaskService {


    /**
     * 新增任务
     *
     * @param params
     */
    boolean add(Map<String, String> params);

    /**
     * 启动
     *
     * @param taskId
     * @return
     */
    boolean start(String taskId);

    /**
     * 停止任务
     *
     * @param taskId
     * @return
     */
    boolean stop(String taskId);


    /**
     * 删除任务
     *
     * @param taskId
     * @return
     */
    boolean deleteTask(String taskId);

    /**
     * 详情任务
     *
     * @param taskId
     * @return
     */
    CommonTask detail(String taskId);

    /**
     * 任务列表
     *
     * @param type
     * @return
     */
    List<CommonTask> list(String type);


}