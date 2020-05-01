package org.dbsyncer.common.task;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Result {

    // 读取数据
    private List<Map<String, Object>> data;

    // 错误数据
    private Queue<Map<String, Object>> failData;

    // 错误数
    private AtomicLong fail;

    // 错误日志
    private StringBuffer error;

    public Result() {
        init();
    }

    public Result(List<Map<String, Object>> data) {
        init();
        this.data = data;
    }

    private void init(){
        this.failData = new ConcurrentLinkedQueue<>();
        this.fail = new AtomicLong(0);
        this.error = new StringBuffer();
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public Queue<Map<String, Object>> getFailData() {
        return failData;
    }

    public AtomicLong getFail() {
        return fail;
    }

    public StringBuffer getError() {
        return error;
    }

}