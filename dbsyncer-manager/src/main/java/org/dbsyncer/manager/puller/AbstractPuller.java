package org.dbsyncer.manager.puller;

import org.dbsyncer.manager.event.ClosedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public abstract class AbstractPuller implements Puller {

    @Autowired
    private ApplicationContext applicationContext;

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}