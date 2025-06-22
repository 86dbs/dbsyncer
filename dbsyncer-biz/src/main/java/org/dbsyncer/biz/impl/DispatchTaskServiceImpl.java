/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.DispatchTaskService;
import org.dbsyncer.biz.dispatch.AbstractDispatchTask;
import org.dbsyncer.biz.dispatch.DispatchTask;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 任务调度实现
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-12 23:45
 */
@Component
public class DispatchTaskServiceImpl implements DispatchTaskService {

    @Resource
    private ThreadPoolTaskExecutor dispatchTaskExecutor;

    private final Set<String> active = new CopyOnWriteArraySet<>();

    @Override
    public void execute(DispatchTask task) {
        // 幂等
        synchronized (active) {
            if (active.contains(task.getUniqueId())) {
                return;
            }
        }
        if (task instanceof AbstractDispatchTask) {
            AbstractDispatchTask adt = (AbstractDispatchTask) task;
            adt.setActive(active);
            dispatchTaskExecutor.execute(adt);
        }
    }
}