package org.dbsyncer.manager.event;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 预加载完成事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-08-26 22:45
 */
public final class PreloadCompletedEvent extends ApplicationContextEvent {

    /**
     * Create a new ContextStartedEvent.
     *
     * @param source the {@code ApplicationContext} that the event is raised for (must not be {@code null})
     */
    public PreloadCompletedEvent(ApplicationContext source) {
        super(source);
    }

}