package org.dbsyncer.common.event;

import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-11 22:50
 */
public interface Event {

    /**
     * 数据变更事件
     *
     * @param event  事件
     * @param before 变化前
     * @param after  变化后
     */
    void changedEvent(String event, Map<String, Object> before, Map<String, Object> after);

}