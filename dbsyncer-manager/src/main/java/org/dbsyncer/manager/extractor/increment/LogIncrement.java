package org.dbsyncer.manager.extractor.increment;

import org.dbsyncer.manager.extractor.AbstractIncrement;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ListenerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-08 00:31
 */
@Component
public class LogIncrement extends AbstractIncrement {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void run(ListenerConfig listenerConfig, Connector connector) {
        for (int i = 0; i < 10; i++) {
            try {
                logger.info("模拟监听任务");
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void close() {
        logger.info("关闭模拟监听任务");

    }
}
