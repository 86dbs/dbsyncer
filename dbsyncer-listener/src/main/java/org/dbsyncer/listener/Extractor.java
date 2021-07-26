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
     * 数据变更事件
     *
     * @param event
     */
    void changedEvent(RowChangedEvent event);

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