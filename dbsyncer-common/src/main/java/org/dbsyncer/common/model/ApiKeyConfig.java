/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * API密钥配置（客户端凭证配置）
 * <p>
 * 支持多版本密钥管理，用于平滑轮换。
 * 客户端使用此密钥进行身份认证，获取JWT Token。
 * </p>
 *
 * @author 穿云
 * @version 2.0.0
 */
public class ApiKeyConfig {

    /**
     * 默认最大保留的密钥版本数量
     */
    public static final int DEFAULT_MAX_VERSION_SIZE = 5;

    /**
     * API密钥可以有多个版本，支持平滑轮换
     */
    private final List<SecretVersion> secretVersions = new ArrayList<>();

    /**
     * 最大保留的密钥版本数量（默认5个）
     */
    private int maxVersionSize = DEFAULT_MAX_VERSION_SIZE;

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
