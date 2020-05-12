package org.dbsyncer.manager.puller.increment;

import org.dbsyncer.manager.puller.AbstractIncrement;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ListenerConfig;
import org.springframework.stereotype.Component;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-08 00:32
 */
@Component
public class TimingIncrement extends AbstractIncrement {

    @Override
    protected void run(ListenerConfig listenerConfig, Connector connector) {

    }

    @Override
    protected void close() {

    }
}