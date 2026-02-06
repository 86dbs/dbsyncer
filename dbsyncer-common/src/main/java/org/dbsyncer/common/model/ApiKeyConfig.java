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
     * API密钥可以有多个版本，支持平滑轮换
     */
    private final List<SecretVersion> secrets = new ArrayList<>();

    public List<SecretVersion> getSecrets() {
        return secrets;
    }
}
