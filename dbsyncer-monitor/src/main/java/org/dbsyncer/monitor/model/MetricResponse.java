package org.dbsyncer.monitor.model;

import java.util.List;

public class MetricResponse {

    private String name;

    private List<Sample> measurements;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Sample> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<Sample> measurements) {
        this.measurements = measurements;
    }
}