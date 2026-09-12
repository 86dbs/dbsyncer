/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.TaskRunner;

/**
 * 单机控制面：本机即执行者，调度方法空操作。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneService implements ClusterService {

    private TaskRunner taskRunner;

    @Override
    public void bindTaskRunner(TaskRunner runner) {
        this.taskRunner = runner;
    }

    @Override
    public void start(String taskId, String model, boolean autoRecovery) {
        taskRunner.start(taskId, autoRecovery);
    }

    @Override
    public void stop(String taskId) {
        taskRunner.stop(taskId);
    }

}
