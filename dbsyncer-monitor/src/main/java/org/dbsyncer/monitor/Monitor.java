package org.dbsyncer.monitor;

import org.dbsyncer.monitor.enums.MetricEnum;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
public interface Monitor {

    boolean isAlive(String id);

    List<MetricEnum> getMetricEnumAll();

    Map getThreadInfo();
}