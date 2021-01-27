package org.dbsyncer.common.event;

import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-11 22:50
 */
public interface Event {

    /**
     * 日志数据变更事件
     *
     * @param rowChangedEvent
     */
    default void changedLogEvent(RowChangedEvent rowChangedEvent) {
        // nothing to do
    }

    /**
     * 定时数据变更事件
     *
     * @param rowChangedEvent
     */
    default void changedQuartzEvent(RowChangedEvent rowChangedEvent){
        // nothing to do
    }

    /**
     * 写入增量点事件
     *
     * @param map
     */
    void flushEvent(Map<String, String> map);

    /**
     * 强制写入增量点事件
     *
     * @param map
     */
    void forceFlushEvent(Map<String,String> map);

    /**
     * 异常事件
     *
     * @param e
     */
    void errorEvent(Exception e);

    /**
     * 中断异常
     *
     * @param e
     */
    void interruptException(Exception e);

}