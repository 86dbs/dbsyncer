package org.dbsyncer.monitor;

import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.model.MetricResponse;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
public interface Monitor {

    boolean isAlive(String id);

    List<MetricEnum> getMetricEnumAll();

    List<MetricResponse> getThreadPoolInfo();
}