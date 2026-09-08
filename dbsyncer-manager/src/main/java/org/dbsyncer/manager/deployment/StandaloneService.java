/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.TaskRunner;
import org.springframework.util.Assert;

/**
 * 单机控制面：本机即执行者，调度方法空操作。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneService implements ClusterService {

    private volatile TaskRunner taskRunner;

    @Override
    public void bindTaskRunner(TaskRunner runner) {
        this.taskRunner = runner;
    }

    @Override
    public void start(String taskId, String model, boolean autoRecovery) {
        getTaskRunner().start(taskId, autoRecovery);
    }

    @Override
    public void stop(String taskId) {
        getTaskRunner().stop(taskId);
    }

    @Override
    public boolean execute(String taskId, boolean autoRecovery) {
        getTaskRunner().start(taskId, autoRecovery);
        return true;
    }

    @Override
    public void stopExecute(String taskId) {
        getTaskRunner().stop(taskId);
    }


    private TaskRunner getTaskRunner() {
        TaskRunner runner = taskRunner;
        Assert.notNull(runner, "本机任务执行器未绑定");
        return runner;
    }
}
