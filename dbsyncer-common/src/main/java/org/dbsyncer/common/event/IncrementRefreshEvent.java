package org.dbsyncer.common.event;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

public class IncrementRefreshEvent extends ApplicationContextEvent {

    private String metaId;

    /**
     * Create a new ContextStartedEvent.
     *
     * @param source the {@code ApplicationContext} that the event is raised for (must not be {@code null})
     */
    public IncrementRefreshEvent(ApplicationContext source, String metaId) {
        super(source);
        this.metaId = metaId;
    }

    public String getMetaId() {
        return metaId;
    }
}