package org.dbsyncer.listener.quartz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-24 22:06
 */
@Component
public class ScheduledTaskServiceImpl implements ScheduledTaskService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 定时任务线程池
     */
    @Autowired
    private ThreadPoolTaskScheduler taskScheduler;

    private Map<String, ScheduledFuture> map = new ConcurrentHashMap<>();

    @Override
    public void start(String key, String cron, ScheduledTaskJob job) {
        //校验任务key是否已经启动
        final ScheduledFuture scheduledFuture = map.get(key);
        if (null != scheduledFuture && !scheduledFuture.isCancelled()) {
            logger.warn(">>>>>> 当前任务已经启动，无需重复启动！");
            return;
        }
        //获取需要定时调度的接口
        map.putIfAbsent(key, taskScheduler.schedule(job, (trigger) -> new CronTrigger(cron).nextExecutionTime(trigger) ));
    }

    @Override
    public void start(String key, long period, ScheduledTaskJob job) {
        //校验任务key是否已经启动
        final ScheduledFuture scheduledFuture = map.get(key);
        if (null != scheduledFuture && !scheduledFuture.isCancelled()) {
            logger.warn(">>>>>> 当前任务已经启动，无需重复启动！");
            return;
        }
        //获取需要定时调度的接口
        map.putIfAbsent(key, taskScheduler.scheduleAtFixedRate(job, period));
    }

    @Override
    public void stop(String key) {
        ScheduledFuture job = map.get(key);
        if (null != job) {
            logger.info(">>>>>> 进入停止任务 {}  >>>>>>", key);
            job.cancel(true);
            map.remove(key);
        }
    }

}