package org.dbsyncer.manager.extractor;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.parser.model.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public abstract class AbstractExtractor implements Extractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationContext applicationContext;

    protected abstract void doTask(Mapping mapping);

    @Override
    public void asyncStart(Mapping mapping) {
        String metaId = mapping.getMetaId();
        logger.info("启动任务:{}", metaId);

        this.doTask(mapping);

        close(metaId);
    }

    @Override
    public void asyncClose(String metaId) {
        close(metaId);
    }

    private void close(String metaId) {
        logger.info("结束任务:{}", metaId);
        applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
    }

}