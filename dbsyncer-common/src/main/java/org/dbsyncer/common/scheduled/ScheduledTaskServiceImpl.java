package org.dbsyncer.common.scheduled;

import org.dbsyncer.common.CommonException;
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
        apply(key, () -> taskScheduler.schedule(job, (trigger) -> new CronTrigger(cron).nextExecutionTime(trigger)));
    }

    @Override
    public void start(String key, long period, ScheduledTaskJob job) {
        apply(key, () -> taskScheduler.scheduleAtFixedRate(job, period));
    }

    @Override
    public void stop(String key) {
        ScheduledFuture job = map.get(key);
        if (null != job) {
            job.cancel(true);
            map.remove(key);
        }
    }

    private void apply(String key, ScheduledFutureMapper scheduledFutureMapper) {
        final ScheduledFuture scheduledFuture = map.get(key);
        if (null != scheduledFuture && !scheduledFuture.isCancelled()) {
            String msg = String.format(">>>>>> 任务已启动 %s  >>>>>>", key);
            logger.error(msg);
            throw new CommonException(msg);
        }
        map.putIfAbsent(key, scheduledFutureMapper.apply());
    }

    private interface ScheduledFutureMapper {
        /**
         * 返回定时任务
         *
         * @return
         */
        ScheduledFuture apply();
    }

}