package org.dbsyncer.biz.model;

public final class Sample {

    private String statistic;

    private Object value;

    public Sample(String statistic, Object value) {
        this.statistic = statistic;
        this.value = value;
    }

    public String getStatistic() {
        return statistic;
    }

    public void setStatistic(String statistic) {
        this.statistic = statistic;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
