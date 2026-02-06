package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.metric.AbstractMetricDetailFormatter;
import org.dbsyncer.biz.vo.MetricResponseVO;
import org.dbsyncer.biz.model.Sample;

public final class DoubleRoundMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVO vo) {
        Sample sample = vo.getMeasurements().get(0);
        long round = Math.round((Double) sample.getValue());
        vo.setDetail(String.valueOf(round));
    }

}