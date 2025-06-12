/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.TaskSchedulerService;
import org.dbsyncer.biz.scheduler.Task;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 任务调度实现
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-12 23:45
 */
@Component
public class TaskSchedulerServiceImpl implements TaskSchedulerService {

    private BlockingQueue<Task> queue;

    @PostConstruct
    public void init() {
        // todo 自定义线程任务数
        queue = new LinkedBlockingQueue<>(1024);
    }

    @Override
    public void submit(Task task) {
        if (!queue.offer(task)) {
            throw new BizException(String.format("submit task[%s] failed.", task.getType().getName()));
        }
    }
}