package org.dbsyncer.biz.enums;

/**
 * 缓存执行器指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/23 0:19
 */
public enum BufferActuatorMetricEnum {

    /**
     * 持久化执行器
     */
    STORAGE("buffer.actuator.storage", "持久化执行器", ""),
    /**
     * 通用执行器
     */
    GENERAL("buffer.actuator.general", "通用执行器", ""),
    /**
     * 表执行器
     */
    TABLE_GROUP("buffer.actuator.table.group", "表执行器", "");

    private final String code;
    private final String group;
    private final String metricName;

    BufferActuatorMetricEnum(String code, String group, String metricName) {
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
