/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * 配置版本信息
 *
 * @version 1.0.0
 * @Author 穿云
 * @Date 2026-02-06 08:30
 */
public abstract class ConfigVersion {

    /**
     * 版本号（从1开始递增）
     */
    private int version;

    /**
     * 创建时间（毫秒时间戳）
     */
    private Long createTime;

    /**
     * 是否启用
     */
    private boolean enabled = true;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
