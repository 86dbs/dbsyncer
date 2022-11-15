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
     * @param event
     */
    void changedEvent(RowChangedEvent event);

    /**
     * 写入增量点事件
     *
     * @param snapshot
     */
    void flushEvent(Map<String, String> snapshot);

    /**
     * 强制写入增量点事件
     *
     * @param snapshot
     */
    void forceFlushEvent(Map<String,String> snapshot);

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