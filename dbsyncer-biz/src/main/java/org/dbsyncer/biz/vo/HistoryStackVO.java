/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HistoryStackVO {

    private List<Object> name;

    private List<Object> value;

    private double average;

    public HistoryStackVO() {
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

    public void addValue(Object value) {
        this.value.add(value);
    }

    public void addName(String name) {
        this.name.add(name);
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }
}
