/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.metric.MetricGroupProcessor;
import org.dbsyncer.biz.vo.MetricResponseVO;
import org.dbsyncer.common.util.StringUtil;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public final class MetricGroupProcessorImpl implements MetricGroupProcessor {

    @Override
    public List<MetricResponseVO> process(List<MetricResponseVO> metrics) {
        Map<String, MetricResponseVO> group = new LinkedHashMap<>();
        Iterator<MetricResponseVO> iterator = metrics.iterator();
        while (iterator.hasNext()) {
            MetricResponseVO metric = iterator.next();
            // 应用性能指标
            MetricEnum metricEnum = MetricEnum.getMetric(metric.getCode());
            if (metricEnum != null) {
                switch (metricEnum) {
                    case THREADS_LIVE:
                    case THREADS_PEAK:
                        buildMetricResponseVo(group, metricEnum.getGroup(), metricEnum.getMetricName(), metric.getDetail());
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }
        }
        metrics.addAll(0, group.values());
        group.clear();
        return metrics;
    }

    private void buildMetricResponseVo(Map<String, MetricResponseVO> group, String groupName, String metricName, String detail) {
        MetricResponseVO vo = getMetricResponseVo(group, groupName);
        vo.setDetail(vo.getDetail() + metricName + StringUtil.COLON + detail + StringUtil.SPACE);
    }

    private MetricResponseVO getMetricResponseVo(Map<String, MetricResponseVO> group, String groupName) {
        return group.compute(groupName, (k, v) -> {
            if (v == null) {
                MetricResponseVO responseVo = new MetricResponseVO();
                responseVo.setGroup(groupName);
                responseVo.setMetricName(StringUtil.EMPTY);
                responseVo.setDetail(StringUtil.EMPTY);
                return responseVo;
            }
            return v;
        });
    }
}