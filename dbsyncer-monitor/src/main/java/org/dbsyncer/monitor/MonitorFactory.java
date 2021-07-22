package org.dbsyncer.monitor;

import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public Map getThreadInfo() {
        Map map = new HashMap();
        if (taskExecutor instanceof ThreadPoolTaskExecutor) {
            ThreadPoolTaskExecutor threadTask = (ThreadPoolTaskExecutor) taskExecutor;
            ThreadPoolExecutor threadPoolExecutor = threadTask.getThreadPoolExecutor();

            map.put("已提交", threadPoolExecutor.getTaskCount());
            map.put("已完成", threadPoolExecutor.getCompletedTaskCount());
            map.put("处理中", threadPoolExecutor.getActiveCount());
            map.put("排队中", threadPoolExecutor.getQueue().size());
            map.put("队列长度", threadPoolExecutor.getQueue().remainingCapacity());
        }
        return map;
    }

}