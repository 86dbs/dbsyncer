package org.dbsyncer.biz.vo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HistoryStackVo {

    private List<Object> name;

    private List<Object> value;

    public HistoryStackVo() {
        this.name = new CopyOnWriteArrayList<>();
        this.value = new CopyOnWriteArrayList<>();
    }

    public List<Object> getName() {
        return name;
    }

    public void setName(List<Object> name) {
        this.name = name;
    }

    public List<Object> getValue() {
        return value;
    }

    public void setValue(List<Object> value) {
        this.value = value;
    }
}