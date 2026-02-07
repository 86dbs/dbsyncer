package org.dbsyncer.biz.enums;

/**
 * 安全信息枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/26 23:21
 */
public enum SafeInfoEnum {

    /**
     * 密码
     */
    PASSWORD("password"),
    /**
     * API密钥
     */
    API_SECRET("apiSecret"),
    /**
     * RSA私钥
     */
    RSA_PRIVATE_KEY("rsaPrivateKey"),
    /**
     * RSA公钥
     */
    RSA_PUBLIC_KEY("rsaPublicKey");

    private final String code;

    SafeInfoEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}