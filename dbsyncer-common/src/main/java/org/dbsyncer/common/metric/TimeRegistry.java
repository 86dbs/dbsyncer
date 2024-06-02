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

    private Map<String, TimeMetric> metricMap = new ConcurrentHashMap<>();

    public TimeMetric meter(String name) {
        return metricMap.computeIfAbsent(name, k -> new TimeMetric());
    }
}