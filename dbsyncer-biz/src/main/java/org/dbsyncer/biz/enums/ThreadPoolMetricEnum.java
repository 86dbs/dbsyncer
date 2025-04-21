package org.dbsyncer.biz.enums;

/**
 * 线程池指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/23 0:19
 */
public enum ThreadPoolMetricEnum {

    /**
     * 核心线程
     */
    CORE_SIZE("线程"),
    /**
     * 排队中
     */
    QUEUE_UP("排队中"),
    /**
     * 处理中
     */
    ACTIVE("处理中"),
    /**
     * 已完成
     */
    COMPLETED("完成"),
    /**
     * 空闲队列
     */
    REMAINING_CAPACITY("空闲队列");

    private final String metricName;

    ThreadPoolMetricEnum(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricName() {
        return metricName;
    }
}