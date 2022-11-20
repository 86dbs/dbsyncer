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

    private int refreshInterval = 5;

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(int refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

}