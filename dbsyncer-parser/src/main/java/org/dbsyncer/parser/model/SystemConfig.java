package org.dbsyncer.parser.model;

import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * 系统配置
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-29 20:13
 */
public class SystemConfig extends ConfigModel {

    public SystemConfig() {
        super.setType(ConfigConstant.SYSTEM);
    }

    /**
     * 同步数据过期时间（天）
     */
    private int expireDataDays = 7;

    /**
     * 系统日志过期时间（天）
     */
    private int expireLogDays = 30;

    /**
     * 刷新页面间隔（秒）
     */
    private int refreshIntervalSeconds = 5;

    /**
     * 是否启用CDN加速访问静态资源(false-禁用；true-启动)
     */
    private boolean enableCDN;

    public int getExpireDataDays() {
        return expireDataDays;
    }

    public void setExpireDataDays(int expireDataDays) {
        this.expireDataDays = expireDataDays;
    }

    public int getExpireLogDays() {
        return expireLogDays;
    }

    public void setExpireLogDays(int expireLogDays) {
        this.expireLogDays = expireLogDays;
    }

    public int getRefreshIntervalSeconds() {
        return refreshIntervalSeconds;
    }

    public void setRefreshIntervalSeconds(int refreshIntervalSeconds) {
        this.refreshIntervalSeconds = refreshIntervalSeconds;
    }

    public boolean isEnableCDN() {
        return enableCDN;
    }

    public void setEnableCDN(boolean enableCDN) {
        this.enableCDN = enableCDN;
    }
}