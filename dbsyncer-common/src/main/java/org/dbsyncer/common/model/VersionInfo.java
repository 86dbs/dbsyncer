/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * 版本信息
 *
 * @version 1.0.0
 * @Author 穿云
 * @Date 2026-01-22 00:30
 */
public final class VersionInfo {

    /**
     * 大版本-小版本-编号-年-月-日-序号
     */
    private long version;

    private String appName;

    private long createTime;

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}
