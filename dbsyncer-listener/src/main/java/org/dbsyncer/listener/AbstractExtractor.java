package org.dbsyncer.listener;

import org.dbsyncer.common.event.Event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 20:35
 */
public abstract class AbstractExtractor implements Extractor {

    private List<Event> watcher = new CopyOnWriteArrayList<>();;

    /**
     * 订阅事件
     */
    public void attach(Event event) {
        watcher.add(event);
    }

    /**
     * 通知关闭事件
     */
    public void notifyEvent() {
        watcher.forEach(w -> w.changedEvent());
    }


}
