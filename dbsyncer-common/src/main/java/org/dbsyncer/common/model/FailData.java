package org.dbsyncer.common.model;

import java.util.List;

/**
 * <p></p>
 *
 * @author yjwang
 * @date 2022/5/10 19:01
 */
public class FailData<T> {

    private List<T> failList;

    private String error;

    public FailData() {
        super();
    }

    public FailData(List<T> failList, String error) {
        super();
        this.failList = failList;
        this.error = error;
    }

    public void setFailList(List<T> failList) {
        this.failList = failList;
    }

    public List<T> getFailList() {
        return failList;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }
}

