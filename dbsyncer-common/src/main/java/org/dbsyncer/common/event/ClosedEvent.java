package org.dbsyncer.common.event;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 任务关闭事件
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020-04-26 22:45
 */
public final class ClosedEvent extends ApplicationContextEvent {

    private final String taskId;

    public ClosedEvent(ApplicationContext source, String metaId) {
        super(source);
        this.taskId = metaId;
    }

//    public String getMetaId() {
//        return metaId;
//    }
//

    public String getTaskId() {
        return taskId;
    }
}
