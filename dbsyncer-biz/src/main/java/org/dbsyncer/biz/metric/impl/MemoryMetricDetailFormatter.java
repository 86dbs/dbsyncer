package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.metric.AbstractMetricDetailFormatter;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.biz.model.Sample;

public final class MemoryMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVo vo) {
        Sample sample = vo.getMeasurements().get(0);
        Double value = (Double) sample.getValue();
        vo.setDetail(String.format("%.2fMB", (value / 1024 / 1024)));
    }

}