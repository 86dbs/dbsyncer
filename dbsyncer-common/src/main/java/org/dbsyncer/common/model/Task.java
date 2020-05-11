package org.dbsyncer.common.model;

import org.dbsyncer.common.event.Event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Task {

    private String id;

    private StateEnum state;

    private long beginTime;

    private long endTime;

    private List<Event> watcher;

    public Task() {
    }

    public Task(String id) {
        this.id = id;
        this.state = StateEnum.RUNNING;
        watcher = new CopyOnWriteArrayList<>();
    }

    public void stop() {
        this.state = StateEnum.STOP;
    }

    /**
     * 订阅事件
     */
    public void attachClosedEvent(Event event) {
        watcher.add(event);
    }

    /**
     * 通知关闭事件
     */
    public void notifyClosedEvent() {
        watcher.forEach(w -> w.closedEvent());
    }

    public boolean isRunning() {
        return StateEnum.RUNNING == state;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public enum StateEnum {
        /**
         * 运行
         */
        RUNNING,
        /**
         * 停止
         */
        STOP;
    }

}