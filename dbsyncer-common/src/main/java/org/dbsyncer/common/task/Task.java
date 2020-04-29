package org.dbsyncer.common.task;

public class Task {

    private String id;

    private StateEnum state;

    private long beginTime;

    private long endTime;

    public Task(String id) {
        this.id = id;
        this.state = StateEnum.RUNNING;
    }

    public void stop() {
        this.state = StateEnum.STOP;
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

    public enum StateEnum{
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