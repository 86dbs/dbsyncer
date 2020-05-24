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

    /**
     * 存放已经启动的任务map
     */
    private Map<String, ScheduledFuture> map = new ConcurrentHashMap<>();

    /**
     * 根据任务key 启动任务
     */
    @Override
    public void start(ScheduledTask task) {
        logger.info(">>>>>> 启动任务 {} 开始 >>>>>>", task);
        //校验任务key是否已经启动
        String key = task.getKey();
        if (this.isStart(key)) {
            logger.info(">>>>>> 当前任务已经启动，无需重复启动！");
        }
        // 定时表达式
        String taskCron = task.getCron();
        // delegate
        ScheduledTaskJob job = task.getJob();
        //获取需要定时调度的接口
        map.putIfAbsent(key, taskScheduler.schedule(job,
                (triggerContext) -> new CronTrigger(taskCron).nextExecutionTime(triggerContext)
        ));
    }

    /**
     * 根据 key 停止任务
     */
    @Override
    public void stop(String taskKey) {
        logger.info(">>>>>> 进入停止任务 {}  >>>>>>", taskKey);
        ScheduledFuture job = map.get(taskKey);
        if (null != job) {
            job.cancel(true);
        }
    }

    /**
     * 任务是否已经启动
     */
    private boolean isStart(String key) {
        final ScheduledFuture job = map.get(key);
        return null != job && job.isCancelled();
    }

}