package org.dbsyncer.monitor.enums;

/**
 * 线程池指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/23 0:19
 */
public enum ThreadPoolMetricEnum {

    /**
     * 已提交
     */
    TASK_SUBMITTED("thread.pool.task.submitted", "线程池", "已提交"),
    /**
     * 排队中
     */
    QUEUE_UP("thread.pool.queue.up", "线程池", "排队中"),
    /**
     * 处理中
     */
    ACTIVE("thread.pool.active", "线程池", "处理中"),
    /**
     * 已完成
     */
    COMPLETED("thread.pool.completed", "线程池", "已完成"),
    /**
     * 空闲队列
     */
    REMAINING_CAPACITY("thread.pool.remaining.capacity", "线程池", "空闲队列");

    private String code;
    private String group;
    private String metricName;

    ThreadPoolMetricEnum(String code, String group, String metricName) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
    }

    public String getCode() {
        return code;
    }

    public String getGroup() {
        return group;
    }

    public String getMetricName() {
        return metricName;
    }
}
