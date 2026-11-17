/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.AgentOffsetConstant;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Agent 采集监听：只绑定增量通道，不连接源库日志；负责把事件位点写入 meta.snapshot。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public final class AgentCaptureListener implements Listener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int FLUSH_DELAYED_SECONDS = 20;

    private Watcher watcher;
    private Map<String, String> snapshot;
    private String taskId;
    /**
     * 最近一次 refreshEvent 写入内存位点的时间；flush 按此节流，不依赖 meta.updateTime
     * （incrementMeta 不刷新 updateTime，会导致启动 20s 后位点只更新内存不落库）。
     */
    private volatile long lastRefreshTime;

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
        forceFlushEvent();
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
        // snapshot 允许为空 Map：Agent 位点键首次写入前 map 可能尚无元素，不能用 isEmpty 拦截。
        if (offset == null || snapshot == null) {
            return;
        }
        Object position = offset.getPosition();
        if (position instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) position;
            put(AgentOffsetConstant.REDO_SCN, map.get("scn"));
            put(AgentOffsetConstant.REDO_SEQUENCE, map.get("sequence"));
            put(AgentOffsetConstant.REDO_BLOCK, map.get("block"));
            put(AgentOffsetConstant.REDO_OFFSET, map.get("offset"));
            put(AgentOffsetConstant.REDO_PATH, map.get("path"));
        } else if (position != null) {
            put(AgentOffsetConstant.REDO_SCN, position);
        }
        if (StringUtil.isNotBlank(offset.getNextFileName())) {
            snapshot.put(AgentOffsetConstant.REDO_PATH, offset.getNextFileName());
        }
        lastRefreshTime = System.currentTimeMillis();
    }

    @Override
    public void flushEvent() {
        if (watcher == null || snapshot == null || snapshot.isEmpty()) {
            return;
        }
        // 近 20s 内有位点刷新才落库，避免无事件时空刷；用 lastRefreshTime 而非 meta.updateTime。
        if (lastRefreshTime <= 0L
                || System.currentTimeMillis() - lastRefreshTime > FLUSH_DELAYED_SECONDS * 1000L) {
            return;
        }
        logger.info("Agent snapshot taskId={}, snapshot={}", taskId, snapshot);
        watcher.flushEvent(snapshot);
    }

    @Override
    public void forceFlushEvent() {
        if (watcher == null || snapshot == null || snapshot.isEmpty()) {
            return;
        }
        logger.info("Agent force flush snapshot taskId={}, snapshot={}", taskId, snapshot);
        watcher.flushEvent(snapshot);
    }

    @Override
    public void errorEvent(Exception e) {
        if (watcher != null) {
            watcher.errorEvent(e);
        }
    }

    private void put(String key, Object value) {
        if (value == null) {
            return;
        }
        String text = String.valueOf(value).trim();
        if (StringUtil.isBlank(text) || "null".equalsIgnoreCase(text)) {
            return;
        }
        snapshot.put(key, text);
    }
}
