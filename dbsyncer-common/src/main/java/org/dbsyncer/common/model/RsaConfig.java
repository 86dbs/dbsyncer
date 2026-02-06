/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * RSA配置
 *
 * @version 1.0.0
 * @Author 穿云
 * @Date 2026-01-15 08:30
 */
public class RsaConfig {

    /**
     * 最大保留的密钥版本数量
     */
    private int maxVersionSize = 3;

    private final List<RsaVersion> rsaVersions = new ArrayList<>();

    public int getMaxVersionSize() {
        return maxVersionSize;
    }

    public void setMaxVersionSize(int maxVersionSize) {
        this.maxVersionSize = maxVersionSize;
    }

    public List<RsaVersion> getRsaVersions() {
        return rsaVersions;
    }
}