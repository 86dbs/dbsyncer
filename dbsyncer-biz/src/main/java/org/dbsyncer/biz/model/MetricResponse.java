package org.dbsyncer.biz.model;

import java.util.List;

public class MetricResponse {

    private String code;

    private String group;

    private String metricName;

    private List<Sample> measurements;

    public MetricResponse() {
    }

    public MetricResponse(String code, String group, String metricName, List<Sample> measurements) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
        this.measurements = measurements;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public List<Sample> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<Sample> measurements) {
        this.measurements = measurements;
    }
}