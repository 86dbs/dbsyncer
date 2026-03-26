/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.sdk.model.ChangedOffset;

/**
 * 监听器接口，提供实现增量同步功能，支持定时和日志解析
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-21 22:48
 */
public interface Listener {

    /**
     * 初始化
     */
    default void init() {
    };

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
     * 数据变更前置事件
     *
     * @param context
     */
    void changeEventBefore(QuartzListenerContext context);

    /**
     * 数据变更事件
     *
     * @param event
     */
    void changeEvent(ChangedEvent event);

    /**
     * 更新增量点
     *
     * @param offset
     */
    void refreshEvent(ChangedOffset offset);

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
