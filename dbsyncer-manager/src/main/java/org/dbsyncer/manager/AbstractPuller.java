package org.dbsyncer.manager;

import org.dbsyncer.manager.event.ClosedEvent;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;

public abstract class AbstractPuller implements Puller {

    @Resource
    private ApplicationContext applicationContext;

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}