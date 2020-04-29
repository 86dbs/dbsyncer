package org.dbsyncer.common.task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Result {

    private List<Map<String, Object>> data;

    private AtomicLong fail;

    private String error;

    public Result() {
        init();
    }

    public Result(List<Map<String, Object>> data) {
        this.data = data;
        init();
    }

    public Result(String error) {
        this.error = error;
        init();
    }

    private void init(){
        this.fail = new AtomicLong(0);
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public AtomicLong getFail() {
        return fail;
    }

    public void setFail(AtomicLong fail) {
        this.fail = fail;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}