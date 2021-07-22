package org.dbsyncer.monitor;

import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.StatisticEnum;
import org.dbsyncer.monitor.enums.ThreadPoolMetricEnum;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.monitor.model.Sample;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
@Component
public class MonitorFactory implements Monitor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Executor taskExecutor;

    @Override
    @Cacheable(value = "connector", keyGenerator = "cacheKeyGenerator")
    public boolean isAlive(String id) {
        Connector connector = manager.getConnector(id);
        return null != connector ? manager.isAliveConnectorConfig(connector.getConfig()) : false;
    }

    @Override
    public List<MetricEnum> getMetricEnumAll() {
        return Arrays.asList(MetricEnum.values());
    }

    @Override
    public List<MetricResponse> getThreadPoolInfo() {
        ThreadPoolTaskExecutor threadTask = (ThreadPoolTaskExecutor) taskExecutor;
        ThreadPoolExecutor pool = threadTask.getThreadPoolExecutor();

        List<MetricResponse> list = new ArrayList<>();
        list.add(createMetricResponse(ThreadPoolMetricEnum.QUEUE_UP, pool.getQueue().size()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.TASK, pool.getTaskCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.ACTIVE, pool.getActiveCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.COMPLETED, pool.getCompletedTaskCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.REMAINING_CAPACITY, pool.getQueue().remainingCapacity()));
        return list;
    }

    private MetricResponse createMetricResponse(ThreadPoolMetricEnum metricEnum, Object value) {
        return new MetricResponse(metricEnum.getCode(), metricEnum.getGroup(), metricEnum.getMetricName(), Arrays.asList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), value)));
    }
}