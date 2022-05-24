package org.dbsyncer.listener;

import org.dbsyncer.listener.enums.ListenerTypeEnum;

public interface Listener {

    <T> T getExtractor(ListenerTypeEnum listenerTypeEnum, String connectorType, Class<T> valueType) throws IllegalAccessException, InstantiationException;

}