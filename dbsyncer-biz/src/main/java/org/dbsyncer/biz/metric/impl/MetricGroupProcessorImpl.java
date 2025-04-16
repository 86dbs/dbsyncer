/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.metric.impl;

import org.dbsyncer.biz.enums.DiskMetricEnum;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.metric.MetricGroupProcessor;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.common.util.StringUtil;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public final class MetricGroupProcessorImpl implements MetricGroupProcessor {

    @Override
    public List<MetricResponseVo> process(List<MetricResponseVo> metrics) {
        Map<String, MetricResponseVo> group = new LinkedHashMap<>();
        Iterator<MetricResponseVo> iterator = metrics.iterator();
        while (iterator.hasNext()) {
            MetricResponseVo metric = iterator.next();
            // 应用性能指标
            MetricEnum metricEnum = MetricEnum.getMetric(metric.getCode());
            if (metricEnum != null) {
                switch (metricEnum) {
                    case THREADS_LIVE:
                    case THREADS_PEAK:
                    case MEMORY_USED:
                    case MEMORY_COMMITTED:
                    case MEMORY_MAX:
                        buildMetricResponseVo(group, metricEnum.getGroup(), metricEnum.getMetricName(), metric.getDetail());
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }

            // 硬盘
            DiskMetricEnum diskEnum = DiskMetricEnum.getMetric(metric.getCode());
            if (diskEnum != null) {
                buildMetricResponseVo(group, diskEnum.getGroup(), diskEnum.getMetricName(), metric.getDetail());
                iterator.remove();
            }
        }
        metrics.addAll(0, group.values());
        group.clear();
        return metrics;
    }

    private void buildMetricResponseVo(Map<String, MetricResponseVo> group, String groupName, String metricName, String detail) {
        MetricResponseVo vo = getMetricResponseVo(group, groupName);
        vo.setDetail(vo.getDetail() + metricName + StringUtil.COLON + detail + StringUtil.SPACE);
    }

    private MetricResponseVo getMetricResponseVo(Map<String, MetricResponseVo> group, String groupName) {
        return group.compute(groupName, (k, v) -> {
            if (v == null) {
                MetricResponseVo responseVo = new MetricResponseVo();
                responseVo.setGroup(groupName);
                responseVo.setMetricName(StringUtil.EMPTY);
                responseVo.setDetail(StringUtil.EMPTY);
                return responseVo;
            }
            return v;
        });
    }
}