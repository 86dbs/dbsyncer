package org.dbsyncer.biz.metric;

import org.dbsyncer.biz.vo.MetricResponseVO;

public interface MetricDetailFormatter {

    void format(MetricResponseVO vo);

}