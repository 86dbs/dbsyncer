package org.dbsyncer.biz.enums;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.util.StringUtil;

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
    THREADS_LIVE("jvm.threads.live", "应用线程", "活跃"),

    /**
     * 线程峰值
     */
    THREADS_PEAK("jvm.threads.peak", "应用线程", "峰值"),

    /**
     * 内存已用
     */
    MEMORY_USED("jvm.memory.used", "内存", "已用"),

    /**
     * 内存空闲
     */
    MEMORY_COMMITTED("jvm.memory.committed", "内存", "空闲"),

    /**
     * 内存总共
     */
    MEMORY_MAX("jvm.memory.max", "内存", "总共"),

    /**
     * GC
     */
    GC_PAUSE("jvm.gc.pause", "GC", ""),

    /**
     * CPU已用
     */
    CPU_USAGE("system.cpu.usage", "CPU", ""),

    /**
     * 系统环境
     */
    SYSTEM_ENV("system.info", "运行环境", "");

    private final String code;
    private final String group;
    private final String metricName;

    MetricEnum(String code, String group, String metricName) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
    }

    public static MetricEnum getMetric(String code) throws BizException {
        for (MetricEnum e : MetricEnum.values()) {
            if (StringUtil.equals(code, e.getCode())) {
                return e;
            }
        }
        return null;
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