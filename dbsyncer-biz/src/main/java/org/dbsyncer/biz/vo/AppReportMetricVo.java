package org.dbsyncer.biz.vo;

import org.dbsyncer.biz.model.AppReportMetric;

import java.util.List;

public class AppReportMetricVo extends AppReportMetric {

    private List<MetricResponseVo> metrics;

    private HistoryStackVo cpu;

    private HistoryStackVo memory;

    public List<MetricResponseVo> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricResponseVo> metrics) {
        this.metrics = metrics;
    }

    public HistoryStackVo getCpu() {
        return cpu;
    }

    public void setCpu(HistoryStackVo cpu) {
        this.cpu = cpu;
    }

    public HistoryStackVo getMemory() {
        return memory;
    }

    public void setMemory(HistoryStackVo memory) {
        this.memory = memory;
    }
}