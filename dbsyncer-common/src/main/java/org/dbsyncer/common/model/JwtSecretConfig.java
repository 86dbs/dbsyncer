/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * JWT密钥配置（服务端签名密钥配置）
 * <p>
 * 支持密钥版本管理和多个历史密钥，用于平滑轮换。
 * 用于生成和验证JWT Token。
 * </p>
 *
 * @author 穿云
 * @version 2.0.0
 */
public class JwtSecretConfig {

    /**
     * 密钥列表（按版本号存储，用于验证旧Token）
     */
    private final List<JwtSecretVersion> secrets = new ArrayList<>();

    public List<JwtSecretVersion> getSecrets() {
        return secrets;
    }

}
