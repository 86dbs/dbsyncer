package org.dbsyncer.biz.enums;

/**
 * 硬盘指标
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/07/23 0:19
 */
public enum DiskMetricEnum {

    /**
     * 已用
     */
    THRESHOLD("disk.space.threshold", "硬盘", "已用"),

    /**
     * 空闲
     */
    FREE("disk.space.free", "硬盘", "空闲"),
    
    /**
     * 总共
     */
    TOTAL("disk.space.total", "硬盘", "总共"),;

    private String code;
    private String group;
    private String metricName;

    DiskMetricEnum(String code, String group, String metricName) {
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
