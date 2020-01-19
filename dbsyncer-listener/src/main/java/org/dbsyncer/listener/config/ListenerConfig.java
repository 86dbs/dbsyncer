package org.dbsyncer.listener.config;

import org.dbsyncer.listener.enums.ListenerEnum;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 22:36
 */
public class ListenerConfig {

    /**
     * 监听器类型
     * @see ListenerEnum
     */
    private String listenerType;

    public String getListenerType() {
        return listenerType;
    }

    public ListenerConfig setListenerType(String listenerType) {
        this.listenerType = listenerType;
        return this;
    }
}