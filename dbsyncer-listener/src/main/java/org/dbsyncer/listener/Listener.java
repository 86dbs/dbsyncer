package org.dbsyncer.listener;

public interface Listener {

    <T> T getExtractor(String type, Class<T> valueType) throws IllegalAccessException, InstantiationException;

}