/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.service;

import org.dbsyncer.common.model.ConfigModel;

/**
 * 本机任务执行器：由 Spring 注入，供集群/单机控制面拉起或停止 Puller。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-07
 */
public interface TaskManager {

    /**
     * 本机拉起任务执行器。
     */
    void start(ConfigModel configModel, boolean autoRecovery);

    /**
     * 本机停止任务执行器。
     *
     * @param taskId 任务 ID（Mapping ID）
     */
    void stop(String taskId);
}
