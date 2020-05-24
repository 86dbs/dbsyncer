package org.dbsyncer.listener.quartz;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-24 22:01
 */
public class ScheduledTask {

    /**
     * 任务key值 唯一
     */
    private String key;
    /**
     * 任务表达式
     */
    private String cron;
    /**
     * 定时执行job
     */
    private ScheduledTaskJob job;

    public ScheduledTask(String key, String cron, ScheduledTaskJob job) {
        this.key = key;
        this.cron = cron;
        this.job = job;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public ScheduledTaskJob getJob() {
        return job;
    }

    public void setJob(ScheduledTaskJob job) {
        this.job = job;
    }
}
