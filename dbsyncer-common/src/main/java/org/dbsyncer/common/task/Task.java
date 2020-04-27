package org.dbsyncer.common.task;

public class Task {

    private TaskCallBack taskCallBack;

    // 0: 停止；1: 运行
    private int state;
    public static final int STOP = 0;
    public static final int RUNNING = 1;

    public TaskCallBack getTaskCallBack() {
        return taskCallBack;
    }

    public void setTaskCallBack(TaskCallBack taskCallBack) {
        this.taskCallBack = taskCallBack;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public boolean isRunning() {
        return state == RUNNING;
    }

    public void close() {
        taskCallBack.cancel();
    }
}