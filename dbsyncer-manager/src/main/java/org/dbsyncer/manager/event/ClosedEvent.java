package org.dbsyncer.manager.event;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 任务关闭事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 22:45
 */
public final class ClosedEvent extends ApplicationContextEvent {

    private String metaId;

    /**
     * Create a new ContextStartedEvent.
     *
     * @param source the {@code ApplicationContext} that the event is raised for
     *               (must not be {@code null})
     */
    public ClosedEvent(ApplicationContext source, String metaId) {
        super(source);
        this.metaId = metaId;
    }

    public String getMetaId() {
        return metaId;
    }
}
