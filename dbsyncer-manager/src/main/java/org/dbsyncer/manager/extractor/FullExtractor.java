package org.dbsyncer.manager.extractor;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 全量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class FullExtractor implements Extractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Parser parser;

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public void start(Mapping mapping) {
        new Thread(()->{
            String metaId = mapping.getMetaId();
            logger.info("模拟同步...等待5s");
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("同步结束");
            applicationContext.publishEvent(new ClosedEvent(applicationContext, metaId));
        }).start();
    }

    @Override
    public void close(String metaId) {

    }

}