/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;

import java.util.List;
import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2025-05-12 23:36
 */
public interface TaskService<T extends ConfigModel> {

    /**
     * 新增
     */
    default String add(T task){
        return StringUtil.EMPTY;
    }

    /**
     * 修改
     */
    default String edit(T task){
        return StringUtil.EMPTY;
    }

    /**
     * 删除
     */
    default void delete(String id){}

    /**
     * 启动
     */
    default void start(String id){}

    /**
     * 停止
     */
    default void stop(String id){}

    /**
     * 获取任务
     */
    default T get(String id){
        return null;
    }

    /**
     * 任务列表
     */
    default Paging search(Map<String, String> param, CommonTaskTypeEnum commonTaskTypeEnum){
        return null;
    }

    /**
     * 获取所有任务 根据任务类型
     */
    default List<T> getTaskAll(CommonTaskTypeEnum commonTaskTypeEnum){
        return null;
    }

    /**
     * 检查任务是否在本进程执行中（内存集合，防重入）。
     */
    default boolean isRunning(String taskId) {
        return false;
    }

    /**
     * 本机续跑已分配任务（不走用户启动链）。
     *
     * @param id 任务 ID
     */
    default void resumeAssigned(String id) {
    }

    /**
     * 仅停止本进程执行，不改调度行。
     */
    default void stopLocal(String id) {
    }
}
