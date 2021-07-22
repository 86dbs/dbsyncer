package org.dbsyncer.monitor.model;

import org.dbsyncer.monitor.enums.StatisticEnum;

public final class Sample {

    private StatisticEnum statistic;

    private Double value;

    public StatisticEnum getStatistic() {
        return statistic;
    }

    public void setStatistic(StatisticEnum statistic) {
        this.statistic = statistic;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}