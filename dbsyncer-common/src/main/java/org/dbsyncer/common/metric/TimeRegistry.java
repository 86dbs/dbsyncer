/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.metric;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-06-02 22:53
 */
@Component
public final class TimeRegistry {

    public static final String GENERAL_BUFFER_ACTUATOR_TPS = "general.buffer.actuator.tps";

    /**
     * 执行器堆积数（近 1 分钟按秒采样）。
     */
    public static final String GENERAL_BUFFER_ACTUATOR_QUEUE = "general.buffer.actuator.queue";

    private Map<String, TimeMetric> metricMap = new ConcurrentHashMap<>();

    public TimeMetric meter(String name) {
        return metricMap.computeIfAbsent(name, k->new TimeMetric());
    }
}
