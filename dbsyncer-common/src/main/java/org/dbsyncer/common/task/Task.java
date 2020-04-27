package org.dbsyncer.common.task;

public class Task {

    private StateEnum state;

    public Task() {
        this.state = StateEnum.RUNNING;
    }

    public void stop() {
        this.state = StateEnum.STOP;
    }

    public boolean isRunning() {
        return StateEnum.RUNNING == state;
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