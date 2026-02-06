package org.dbsyncer.biz.metric;

import org.dbsyncer.biz.vo.MetricResponseVO;
import org.dbsyncer.common.util.CollectionUtils;

public abstract class AbstractMetricDetailFormatter implements MetricDetailFormatter {

    protected abstract void apply(MetricResponseVO vo);

    @Override
    public void format(MetricResponseVO vo) {
        if (CollectionUtils.isEmpty(vo.getMeasurements())) {
            return;
        }
        apply(vo);
    }
}