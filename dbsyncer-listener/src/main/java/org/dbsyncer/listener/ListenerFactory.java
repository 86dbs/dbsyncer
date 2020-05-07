package org.dbsyncer.listener;


import org.dbsyncer.common.task.Task;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ListenerFactory implements ApplicationContextAware {

    private Map<String, Listener> pull;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        pull = applicationContext.getBeansOfType(Listener.class);
    }

    public void execute(Task task, ListenerConfig listenerConfig, ConnectorConfig connector) {


    }
}
