package org.dbsyncer.monitor;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
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
    public boolean alive(String id) {
        Connector connector = manager.getConnector(id);
        return null != connector ? manager.alive(connector.getConfig()) : false;
    }

    @Override
    public Map getThreadInfo() {
        Map map = new HashMap();
        if(taskExecutor instanceof ThreadPoolTaskExecutor){
            ThreadPoolTaskExecutor threadTask = (ThreadPoolTaskExecutor) taskExecutor;
            ThreadPoolExecutor threadPoolExecutor = threadTask.getThreadPoolExecutor();

            map.put("提交任务数", threadPoolExecutor.getTaskCount());
            map.put("完成任务数", threadPoolExecutor.getCompletedTaskCount());
            map.put("当前有多少线程正在处理任务", threadPoolExecutor.getActiveCount());
            map.put("还剩多少个任务未执行", threadPoolExecutor.getQueue().size());
            map.put("当前可用队列长度", threadPoolExecutor.getQueue().remainingCapacity());
            map.put("当前时间", DateFormatUtil.getCurrentDateTime());
        }
        return map;
    }

}