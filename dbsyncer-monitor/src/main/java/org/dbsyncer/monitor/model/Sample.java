package org.dbsyncer.monitor.model;

public final class Sample {

    private String statistic;

    private Double value;

    public Sample(String statistic, Double value) {
        this.statistic = statistic;
        this.value = value;
    }

    public String getStatistic() {
        return statistic;
    }

    public void setStatistic(String statistic) {
        this.statistic = statistic;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}