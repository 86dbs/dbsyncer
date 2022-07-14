package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(value = "dbsyncer.parser.flush.buffer.actuator.speed.enabled", havingValue = "true")
public final class EnableWriterBufferActuatorStrategy implements ParserStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final double BUFFER_THRESHOLD = 0.8;

    @Autowired
    private BufferActuator writerBufferActuator;

    private double limit;

    @PostConstruct
    private void init() {
        limit = Math.ceil(writerBufferActuator.getQueueCapacity() * BUFFER_THRESHOLD);
    }

    @Override
    public void execute(String tableGroupId, String event, Map<String, Object> data) {
        writerBufferActuator.offer(new WriterRequest(tableGroupId, event, data));

        // 超过容量限制，限制生产速度
        final int size = writerBufferActuator.getQueue().size();
        if (size >= limit) {
            try {
                TimeUnit.SECONDS.sleep(30);
                logger.warn("当前任务队列大小{}已达上限{}，请稍等{}秒", size, limit, 30);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

}