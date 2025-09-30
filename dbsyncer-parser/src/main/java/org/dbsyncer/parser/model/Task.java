package org.dbsyncer.parser.model;

public class Task {

    private String id;

    private StateEnum state;


    private int pageIndex;

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

    // 存储的是 metaId
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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