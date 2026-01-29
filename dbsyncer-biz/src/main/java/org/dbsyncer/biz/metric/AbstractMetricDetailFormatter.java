package org.dbsyncer.biz.metric;

import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.common.util.CollectionUtils;

public abstract class AbstractMetricDetailFormatter implements MetricDetailFormatter {

    protected abstract void apply(MetricResponseVo vo);

    @Override
    public void format(MetricResponseVo vo) {
        if (CollectionUtils.isEmpty(vo.getMeasurements())) {
            return;
        }
        apply(vo);
    }
}