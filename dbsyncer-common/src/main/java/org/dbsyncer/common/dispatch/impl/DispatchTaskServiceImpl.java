/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.common.dispatch.impl;

import org.dbsyncer.common.dispatch.AbstractDispatchTask;
import org.dbsyncer.common.dispatch.DispatchTask;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务调度实现
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-12 23:45
 */
@Component
public class DispatchTaskServiceImpl implements DispatchTaskService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ThreadPoolTaskExecutor dispatchTaskExecutor;

    private final Map<String, DispatchTask> active = new ConcurrentHashMap<>();

    @Override
    public void execute(DispatchTask task) {
        dispatchTaskExecutor.execute(active.compute(task.getUniqueId(), (k, t) -> {
            if (t != null) {
                t.destroy();
                logger.warn("The dispatch task was terminated, {}", k);
            }
            if (task instanceof AbstractDispatchTask) {
                AbstractDispatchTask adt = (AbstractDispatchTask) task;
                adt.setActive(active);
            }
            return task;
        }));
    }

    @Override
    public boolean isRunning(String uniqueId) {
        return active.containsKey(uniqueId);
    }
}