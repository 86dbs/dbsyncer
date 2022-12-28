package org.dbsyncer.manager.puller;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.manager.Puller;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;

public abstract class AbstractPuller implements Puller {

    @Resource
    private ApplicationContext applicationContext;

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}