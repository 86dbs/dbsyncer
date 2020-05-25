package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.quartz.ScheduledTaskJob;
import org.dbsyncer.listener.quartz.ScheduledTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 默认定时抽取
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 20:35
 */
public class DefaultExtractor implements Extractor, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConnectorConfig connectorConfig;
    protected ListenerConfig listenerConfig;
    protected ConnectorFactory connectorFactory;
    protected ScheduledTaskService scheduledTaskService;
    protected Map<String, String> map;
    private List<Event> watcher;
    private String key;
    private AtomicBoolean running = new AtomicBoolean();

    @Override
    public void start() {
        key = UUIDUtil.getUUID();
        String cron = listenerConfig.getCronExpression();
        logger.info("启动定时任务:{} >> {}", key, cron);
        scheduledTaskService.start(key, cron, this);
    }

    @Override
    public void run() {
        if(running.compareAndSet(false, true)){
            // TODO 获取tableGroup
            Map<String, String> command = null;
            int pageIndex = 1;
            int pageSize = 20;
            connectorFactory.reader(connectorConfig, command, pageIndex, pageSize);
        }
    }

    @Override
    public void close() {
        scheduledTaskService.stop(key);
    }

    public void addListener(Event event) {
        if (null != event) {
            if (null == watcher) {
                watcher = new CopyOnWriteArrayList<>();
            }
            watcher.add(event);
        }
    }

    public void clearAllListener() {
        if (null != watcher) {
            watcher.clear();
            watcher = null;
        }
    }

    public void changedEvent(String tableName, String event, List<Object> before, List<Object> after) {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.changedEvent(tableName, event, before, after));
        }
    }

    public void flushEvent() {
        if (!CollectionUtils.isEmpty(watcher)) {
            watcher.forEach(w -> w.flushEvent(map));
        }
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public void setListenerConfig(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setScheduledTaskService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

}