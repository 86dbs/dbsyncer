package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.metric.AbstractMetricDetailFormatter;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.monitor.model.Sample;

import java.math.BigDecimal;

public final class DiskMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVo vo) {
        Sample sample = vo.getMeasurements().get(0);
        BigDecimal decimal = new BigDecimal(String.valueOf(sample.getValue()));
        BigDecimal bt = divide(decimal,0);
        BigDecimal mb = divide(bt,0);
        BigDecimal gb = divide(mb,2);
        vo.setDetail(gb + "GB");
    }

    private BigDecimal divide(BigDecimal d1, int scale) {
        return d1.divide(new BigDecimal("1024"), scale, BigDecimal.ROUND_HALF_UP);
    }

}