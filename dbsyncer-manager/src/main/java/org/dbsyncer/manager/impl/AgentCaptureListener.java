/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.model.ChangedOffset;

/**
 * Agent 采集占位监听：只绑定增量通道，不连接源库日志。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public final class AgentCaptureListener implements Listener {

    private Watcher watcher;

    @Override
    public void start() {
    }

    @Override
    public void close() {
    }

    @Override
    public void register(Watcher watcher) {
        this.watcher = watcher;
    }

    @Override
    public void changeEventBefore(QuartzListenerContext context) {
        if (watcher != null) {
            watcher.changeEventBefore(context);
        }
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        if (watcher != null) {
            watcher.changeEvent(event);
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
    }

    @Override
    public void flushEvent() {
    }

    @Override
    public void forceFlushEvent() {
    }

    @Override
    public void errorEvent(Exception e) {
        if (watcher != null) {
            watcher.errorEvent(e);
        }
    }
}
