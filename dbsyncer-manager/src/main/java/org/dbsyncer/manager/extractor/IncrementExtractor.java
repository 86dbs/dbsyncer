package org.dbsyncer.manager.extractor;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.Listener;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementExtractor implements Extractor {

    @Autowired
    private Listener listener;

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public void start(Mapping mapping) {
        final String metaId = mapping.getMetaId();
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

    @Override
    public void close(String metaId) {

    }

}