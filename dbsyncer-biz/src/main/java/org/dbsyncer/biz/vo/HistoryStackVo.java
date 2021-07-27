package org.dbsyncer.biz.vo;

import java.util.LinkedList;
import java.util.List;

public class HistoryStackVo {

    private List<Object> name;

    private List<Object> value;

    public HistoryStackVo() {
        this.name = new LinkedList<>();
        this.value = new LinkedList<>();
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