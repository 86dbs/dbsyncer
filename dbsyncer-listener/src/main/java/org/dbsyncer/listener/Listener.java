package org.dbsyncer.listener;

public interface Listener {

    <T> T getExtractor(String groupType, String listenerType, Class<T> valueType) throws IllegalAccessException, InstantiationException;

}