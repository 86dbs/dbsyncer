/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.AgentOffsetConstant;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
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
        if (offset == null || CollectionUtils.isEmpty(snapshot)) {
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
    }

    @Override
    public void flushEvent() {
        if (watcher == null || CollectionUtils.isEmpty(snapshot)) {
            return;
        }
        if (watcher.getMetaUpdateTime() > Timestamp.valueOf(LocalDateTime.now().minusSeconds(FLUSH_DELAYED_SECONDS)).getTime()) {
            logger.info("Agent snapshot taskId={}, snapshot={}", taskId, snapshot);
            watcher.flushEvent(snapshot);
        }
    }

    @Override
    public void forceFlushEvent() {
        if (watcher == null || CollectionUtils.isEmpty(snapshot)) {
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
