package org.dbsyncer.listener.kafka;

import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/18 22:19
 */
public class KafkaExtractor extends AbstractExtractor {

    @Override
    public void start() {
        throw new ListenerException("抱歉，kafka监听功能正在开发中...");
    }

    @Override
    public void close() {

    }
}
