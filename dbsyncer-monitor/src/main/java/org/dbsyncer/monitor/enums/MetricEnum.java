package org.dbsyncer.monitor.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.monitor.MonitorException;

/**
 * 系统指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/22 19:19
 */
public enum MetricEnum {

    /**
     * 线程活跃数
     */
    THREADS_LIVE("jvm.threads.live", "线程", "活跃数"),

    /**
     * 线程峰值
     */
    THREADS_PEAK("jvm.threads.peak", "线程", "峰值数"),

    /**
     * 内存已用
     */
    MEMORY_USED("jvm.memory.used", "内存", "已用", "字节"),

    /**
     * 内存空闲
     */
    MEMORY_COMMITTED("jvm.memory.committed", "内存", "空闲", "字节"),

    /**
     * 内存总共
     */
    MEMORY_MAX("jvm.memory.max", "内存", "总共", "字节"),

    /**
     * CPU已用
     */
    CPU_USAGE("system.cpu.usage", "CPU", "已用", "%"),

    /**
     * GC
     */
    GC_PAUSE("jvm.gc.pause", "GC", "已用", "秒");

    private String code;
    private String group;
    private String metricName;
    private String baseUnit;

    MetricEnum(String code, String group, String metricName) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
    }

    MetricEnum(String code, String group, String metricName, String baseUnit) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
        this.baseUnit = baseUnit;
    }

    public static MetricEnum getMetric(String code) throws ConnectorException {
        for (MetricEnum e : MetricEnum.values()) {
            if (StringUtils.equals(code, e.getCode())) {
                return e;
            }
        }
        throw new MonitorException(String.format("Metric code \"%s\" does not exist.", code));
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

    public String getBaseUnit() {
        return baseUnit;
    }
}