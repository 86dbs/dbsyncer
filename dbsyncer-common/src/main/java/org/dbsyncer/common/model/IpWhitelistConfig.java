/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * IP白名单配置
 * 支持单个IP和IP段（CIDR）配置
 *
 * @author 穿云
 * @version 1.0.0
 */
public class IpWhitelistConfig {

    /**
     * 是否启用IP白名单
     */
    private boolean enabled = false;

    /**
     * IP白名单列表
     * 支持格式：
     * - 单个IP: 192.168.1.1
     * - IP段（CIDR）: 192.168.1.0/24
     * - IP范围: 192.168.1.1-192.168.1.100
     */
    private List<String> whitelist;

    /**
     * 是否允许内网IP（127.0.0.1, localhost等）
     */
    private boolean allowLocalhost = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getWhitelist() {
        if (whitelist == null) {
            whitelist = new ArrayList<>();
        }
        return whitelist;
    }

    public void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist;
    }

    /**
     * 添加IP到白名单
     *
     * @param ip IP地址或IP段
     */
    public void addIp(String ip) {
        if (whitelist == null) {
            whitelist = new ArrayList<>();
        }
        if (ip != null && !ip.trim().isEmpty() && !whitelist.contains(ip.trim())) {
            whitelist.add(ip.trim());
        }
    }

    /**
     * 移除IP
     *
     * @param ip IP地址或IP段
     */
    public void removeIp(String ip) {
        if (whitelist != null) {
            whitelist.remove(ip);
        }
    }

    public boolean isAllowLocalhost() {
        return allowLocalhost;
    }

    public void setAllowLocalhost(boolean allowLocalhost) {
        this.allowLocalhost = allowLocalhost;
    }
}
