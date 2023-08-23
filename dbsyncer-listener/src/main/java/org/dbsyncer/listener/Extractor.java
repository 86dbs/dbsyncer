package org.dbsyncer.listener;

import org.dbsyncer.common.event.ChangedEvent;
import org.dbsyncer.common.event.Watcher;

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
     * 注册监听事件（获取增量数据）
     *
     * @param watcher
     */
    void register(Watcher watcher);

    /**
     * 数据变更事件
     *
     * @param event
     */
    void changeEvent(ChangedEvent event);

    /**
     * 更新增量点
     *
     * @param event
     */
    void refreshEvent(ChangedEvent event);

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

}