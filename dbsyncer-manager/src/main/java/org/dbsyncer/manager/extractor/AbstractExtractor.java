package org.dbsyncer.manager.extractor;

import org.dbsyncer.common.event.ClosedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public abstract class AbstractExtractor implements Extractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationContext applicationContext;

    protected void publishClosedEvent(String metaId) {
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}