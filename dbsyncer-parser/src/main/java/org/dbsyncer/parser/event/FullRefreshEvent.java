/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.event;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.model.Task;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 全量翻页后刷新 Meta 进度与计数。
 *
 * @author AE86
 * @version 1.0.0
 */
public final class FullRefreshEvent extends ApplicationContextEvent {

    private final Task task;
    private final Result result;
    private volatile boolean progressCommitted;

    /**
     * @param source 上下文
     * @param task   运行态
     * @param result 本页写入结果（计数在进度落盘成功后累加）
     */
    public FullRefreshEvent(ApplicationContext source, Task task, Result result) {
        super(source);
        this.task = task;
        this.result = result;
    }

    public Task getTask() {
        return task;
    }

    public Result getResult() {
        return result;
    }

    public boolean isProgressCommitted() {
        return progressCommitted;
    }

    public void setProgressCommitted(boolean progressCommitted) {
        this.progressCommitted = progressCommitted;
    }
}
