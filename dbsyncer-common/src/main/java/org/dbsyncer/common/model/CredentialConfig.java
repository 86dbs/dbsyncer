/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 系统凭证配置
 * 支持多版本密钥管理，用于平滑轮换
 *
 * @author 穿云
 * @version 2.0.0
 */
public class CredentialConfig {

    /**
     * 系统凭证可以有多个版本的密钥，支持平滑轮换
     */
    private final List<SecretVersion> secretVersions = new ArrayList<>();

    /**
     * 最大保留的密钥版本数量（默认5个）
     */
    private int maxVersionSize = 5;

    public List<SecretVersion> getSecretVersions() {
        return secretVersions;
    }

    public int getMaxVersionSize() {
        return maxVersionSize;
    }

    public void setMaxVersionSize(int maxVersionSize) {
        this.maxVersionSize = maxVersionSize;
    }
}
