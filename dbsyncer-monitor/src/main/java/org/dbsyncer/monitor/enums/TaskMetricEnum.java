package org.dbsyncer.monitor.enums;

/**
 * 执行任务指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/23 0:19
 */
public enum TaskMetricEnum {

    /**
     * 处理中
     */
    STORAGE_ACTIVE("parser.storage.buffer.actuator.active", "持久化", "处理中"),

    /**
     * 空闲队列
     */
    STORAGE_REMAINING_CAPACITY("parser.storage.buffer.actuator.capacity", "持久化", "空闲队列");

    private String code;
    private String group;
    private String metricName;

    TaskMetricEnum(String code, String group, String metricName) {
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