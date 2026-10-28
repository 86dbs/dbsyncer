package org.dbsyncer.biz.enums;

import org.dbsyncer.common.util.StringUtil;

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
     * 集群密钥
     */
    CLUSTER_SECRET("clusterSecret"),
    /**
     * RSA私钥
     */
    RSA_PRIVATE_KEY("rsaPrivateKey"),
    /**
     * RSA公钥
     */
    RSA_PUBLIC_KEY("rsaPublicKey"),
    /**
     * 企业微信@用户手机号
     */
    WECHAT_AT_MOBILES("wechatAtMobiles"),
    /**
     * 企业微信Webhook地址
     */
    WECHAT_WebhookUrl("wechatWebhookUrl"),
    /**
     * 钉钉@用户手机号
     */
    DINGTALK_AT_MOBILES("dingTalkAtMobiles"),
    /**
     * 钉钉Webhook地址
     */
    DINGTALK_WebhookUrl("dingTalkWebhookUrl");

    private final String code;

    SafeInfoEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    /**
     * 是否安全敏感字段（忽略大小写）。
     */
    public static boolean contains(String key) {
        if (StringUtil.isBlank(key)) {
            return false;
        }
        for (SafeInfoEnum e : values()) {
            if (StringUtil.equalsIgnoreCase(e.code, key)) {
                return true;
            }
        }
        return false;
    }
}
