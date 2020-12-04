package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.event.RowChangedEvent;

public interface Extractor {

    /**
     * 启动定时/日志抽取任务
     */
    void start();

    /**
     * 关闭任务
     */
    void close();

    /**
     * 添加监听器（获取增量数据）
     *
     * @param event
     */
    void addListener(Event event);

    /**
     * 清空监听器
     */
    void clearAllListener();

    /**
     * 定时模式: 监听增量事件
     *
     * @param rowChangedEvent
     */
    void changedQuartzEvent(RowChangedEvent rowChangedEvent);

    /**
     * 日志模式: 监听增量事件
     *
     * @param rowChangedEvent
     */
    void changedLogEvent(RowChangedEvent rowChangedEvent);

    /**
     * 刷新增量点事件
     */
    void flushEvent();

    /**
     * 强制刷新增量点事件
     */
    void forceFlushEvent();

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