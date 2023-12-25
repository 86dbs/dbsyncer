/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.constant.ConfigConstant;

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
     * 是否启用CDN加速访问静态资源(false-关闭; true-开启）
     */
    private boolean enableCDN;

    /**
     * 是否记录全量数据（false-关闭; true-开启）
     */
    private boolean enableStorageWriteFull;

    /**
     * 是否记录同步成功数据（false-关闭; true-开启）
     */
    private boolean enableStorageWriteSuccess;

    /**
     * 是否记录同步失败数据（false-关闭; true-开启）
     */
    private boolean enableStorageWriteFail = true;

    /**
     * 记录同步失败日志最大长度
     */
    private int maxStorageErrorLength = 2048;

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

    public boolean isEnableStorageWriteFull() {
        return enableStorageWriteFull;
    }

    public void setEnableStorageWriteFull(boolean enableStorageWriteFull) {
        this.enableStorageWriteFull = enableStorageWriteFull;
    }

    public boolean isEnableStorageWriteSuccess() {
        return enableStorageWriteSuccess;
    }

    public void setEnableStorageWriteSuccess(boolean enableStorageWriteSuccess) {
        this.enableStorageWriteSuccess = enableStorageWriteSuccess;
    }

    public boolean isEnableStorageWriteFail() {
        return enableStorageWriteFail;
    }

    public void setEnableStorageWriteFail(boolean enableStorageWriteFail) {
        this.enableStorageWriteFail = enableStorageWriteFail;
    }

    public int getMaxStorageErrorLength() {
        return maxStorageErrorLength;
    }

    public void setMaxStorageErrorLength(int maxStorageErrorLength) {
        this.maxStorageErrorLength = maxStorageErrorLength;
    }
}