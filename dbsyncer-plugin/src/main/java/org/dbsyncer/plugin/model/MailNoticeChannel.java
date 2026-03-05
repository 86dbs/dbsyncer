/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

/**
 * 邮件告警配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public final class MailNoticeChannel {

    /**
     * 是否启用
     */
    private boolean enabled;

    /**
     * 邮件账号
     */
    private String account;

    /**
     * 发邮件的Code,不是邮箱密码
     */
    private String code;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
