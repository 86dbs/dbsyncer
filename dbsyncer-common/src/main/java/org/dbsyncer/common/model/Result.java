package org.dbsyncer.common.model;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Result {

    // 读取数据
    private List<Map> data;

    // 错误数据
    private Queue<Map> failData;

    // 错误数
    private AtomicLong fail;

    // 错误日志
    private StringBuffer error;

    public Result() {
        init();
    }

    public Result(List<Map> data) {
        init();
        this.data = data;
    }

    private void init(){
        this.failData = new ConcurrentLinkedQueue<>();
        this.fail = new AtomicLong(0);
        this.error = new StringBuffer();
    }

    public List<Map> getData() {
        return data;
    }

    public Queue<Map> getFailData() {
        return failData;
    }

    public AtomicLong getFail() {
        return fail;
    }

    public StringBuffer getError() {
        return error;
    }

}