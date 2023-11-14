package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.metric.AbstractMetricDetailFormatter;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.biz.model.Sample;

public final class ValueMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVo vo) {
        Sample sample = vo.getMeasurements().get(0);
        vo.setDetail(String.valueOf(sample.getValue()));
    }

}