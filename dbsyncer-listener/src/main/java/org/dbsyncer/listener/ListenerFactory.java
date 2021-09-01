package org.dbsyncer.listener;

import org.dbsyncer.listener.enums.ListenerEnum;
import org.springframework.stereotype.Component;

@Component
public class ListenerFactory implements Listener {

    @Override
    public <T> T getExtractor(String groupType, String listenerType, Class<T> valueType) throws IllegalAccessException, InstantiationException {
        Class<T> clazz = (Class<T>) ListenerEnum.getExtractor(groupType + listenerType);
        return clazz.newInstance();
    }

}