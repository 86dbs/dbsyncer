package org.dbsyncer.listener.extractor;

import org.dbsyncer.listener.DefaultExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class MysqlExtractor extends DefaultExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    boolean running;

    @Override
    public void extract() {
        running = true;
        for (int i = 0; i < 20; i++) {
            if(!running){
                logger.info("中止监听任务");
                break;
            }
            logger.info("模拟监听任务");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void extractTiming() {

    }

    @Override
    public void close() {
        running = false;
    }
}