package org.dbsyncer.parser.model;

public class Task {

    private String id;

    private StateEnum state;

    private int tableGroupIndex;

    private int pageIndex;

    private Object[] cursors;

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

    public int getTableGroupIndex() {
        return tableGroupIndex;
    }

    public void setTableGroupIndex(int tableGroupIndex) {
        this.tableGroupIndex = tableGroupIndex;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public Object[] getCursors() {
        return cursors;
    }

    public void setCursors(Object[] cursors) {
        this.cursors = cursors;
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