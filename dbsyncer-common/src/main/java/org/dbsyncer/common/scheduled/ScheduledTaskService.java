package org.dbsyncer.common.scheduled;

public interface ScheduledTaskService {

    /**
     * 第一位，表示秒，取值0-59
     * 第二位，表示分，取值0-59
     * 第三位，表示小时，取值0-23
     * 第四位，日期天/日，取值1-31
     * 第五位，日期月份，取值1-12
     * 第六位，星期，取值1-7
     * [秒 分 时 日 月 星期]
     *
     * @param key 任务唯一key
     * @param cron 任务表达式
     * @param job 任务实现
     */
    void start(String key, String cron, ScheduledTaskJob job);

    void start(String key, long period, ScheduledTaskJob job, String suffix);

    void start(String cron, ScheduledTaskJob job);

    void start(long period, ScheduledTaskJob job);

    void start(long period, ScheduledTaskJob job, String suffix);

    void stop(String key);

}