package org.dbsyncer.biz.vo;

import org.dbsyncer.monitor.model.AppReportMetric;

import java.util.List;

public class AppReportMetricVo extends AppReportMetric {

    private List<MetricResponseVo> metrics;

    private HistoryStackVo cpu;

    private HistoryStackVo memery;

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

    public HistoryStackVo getMemery() {
        return memery;
    }

    public void setMemery(HistoryStackVo memery) {
        this.memery = memery;
    }
}