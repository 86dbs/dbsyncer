package org.dbsyncer.manager;

import org.dbsyncer.manager.event.ClosedEvent;
import org.springframework.context.ApplicationContext;

public abstract class AbstractPuller implements Puller {

    private final ApplicationContext applicationContext;

    public AbstractPuller(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}