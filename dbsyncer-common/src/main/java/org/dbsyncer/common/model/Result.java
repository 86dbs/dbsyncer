package org.dbsyncer.common.model;

import java.util.LinkedList;
import java.util.List;

public class Result<T> {

    /**
     * 成功数据
     */
    private final List<T> successData = new LinkedList<>();

    /**
     * 错误数据
     */
    private final List<T> failData = new LinkedList<>();

    /**
     * 错误日志
     */
    private StringBuffer error = new StringBuffer();

    /**
     * 驱动表映射关系ID
     */
    private String tableGroupId;

    /**
     * 目标表名称
     */
    private String targetTableGroupName;

    private final Object LOCK = new Object();

    public Result() {
    }

    public Result(List<T> data) {
        this.successData.addAll(data);
    }

    public List<T> getSuccessData() {
        return successData;
    }

    public List<T> getFailData() {
        return failData;
    }

    public StringBuffer getError() {
        return error;
    }

    /**
     * 线程安全添加集合
     *
     * @param failData
     */
    public void addFailData(List failData) {
        synchronized (LOCK) {
            this.failData.addAll(failData);
        }
    }

    /**
     * 线程安全添加集合
     *
     * @param successData
     */
    public void addSuccessData(List successData) {
        synchronized (LOCK) {
            this.successData.addAll(successData);
        }
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public String getTargetTableGroupName() {
        return targetTableGroupName;
    }

    public void setTargetTableGroupName(String targetTableGroupName) {
        this.targetTableGroupName = targetTableGroupName;
    }
}