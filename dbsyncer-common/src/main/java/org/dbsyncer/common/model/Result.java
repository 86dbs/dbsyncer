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
    private final List<FailData<T>> failData = new LinkedList<>();

    /**
     * 错误日志
     */
    private final StringBuffer error = new StringBuffer();

    private final Object LOCK = new Object();

    public Result() {
    }

    public Result(List<T> data) {
        this.successData.addAll(data);
    }

    public List<T> getSuccessData() {
        return successData;
    }

    public List<FailData<T>> getFailData() {
        return failData;
    }

    public StringBuffer getError() {
        return error;
    }

    /**
     * 线程安全添加集合
     *
     * @param failData 失败数据
     */
    public void addFailData(List<FailData<T>> failData) {
        synchronized (LOCK) {
            this.failData.addAll(failData);
        }
    }

    /**
     * 线程安全添加集合
     *
     * @param failData 失败数据
     */
    public void addFailData(FailData<T> failData) {
        synchronized (LOCK) {
            this.failData.add(failData);
        }
    }

    /**
     * 线程安全添加集合
     *
     * @param successData 成功数据
     */
    public void addSuccessData(List<T> successData) {
        synchronized (LOCK) {
            this.successData.addAll(successData);
        }
    }
}