/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-03-06 01:01
 */
public abstract class NoticeChannel {

    /**
     * 是否启用
     */
    private boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
