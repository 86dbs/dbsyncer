package org.dbsyncer.biz.enums;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.util.StringUtil;

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

    private final String code;
    private final String group;
    private final String metricName;

    DiskMetricEnum(String code, String group, String metricName) {
        this.code = code;
        this.group = group;
        this.metricName = metricName;
    }

    public static DiskMetricEnum getMetric(String code) throws BizException {
        for (DiskMetricEnum e : DiskMetricEnum.values()) {
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
