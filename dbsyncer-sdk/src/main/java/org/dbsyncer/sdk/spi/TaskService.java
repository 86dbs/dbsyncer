/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;

import java.util.List;
import java.util.Map;

/**
 * 任务调度服务（ValidateSync / DatabaseSync 等，持久化到 {@code dbsyncer_task}）。
 * <p>运行态权威在 {@code dbsyncer_meta.STATE}；进程内防重入见 {@link #isRunning(String)}。</p>
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-05-12 23:36
 */
public interface TaskService<T extends ConfigModel> {

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
     * 获取所有任务 根据任务类型
     *
     * @return
     */
    List<T> getTaskAll(CommonTaskTypeEnum commonTaskTypeEnum);

    /**
     * 检查任务是否在本进程执行中（内存集合，防重入）。
     *
     * @param taskId
     * @return
     */
    boolean isRunning(String taskId);

}
