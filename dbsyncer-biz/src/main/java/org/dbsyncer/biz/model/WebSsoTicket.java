/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

/**
 * Web 控制台跨节点 SSO 短时票据载荷（与 OpenAPI TokenInfo 分离）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class WebSsoTicket {

    /**
     * 用户名
     */
    private String username;

    /**
     * 角色编码（逗号分隔，与 UserInfo.roleCode 一致）
     */
    private String roleCode;

    /**
     * 一次性票据 ID，用于防重放
     */
    private String jti;

    /**
     * 目标节点 host，格式 {@code ip:httpPort}
     */
    private String targetHost;

    /**
     * 签发时间（毫秒）
     */
    private Long iat;

    /**
     * 过期时间（毫秒）
     */
    private Long exp;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getRoleCode() {
        return roleCode;
    }

    public void setRoleCode(String roleCode) {
        this.roleCode = roleCode;
    }

    public String getJti() {
        return jti;
    }

    public void setJti(String jti) {
        this.jti = jti;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    public Long getIat() {
        return iat;
    }

    public void setIat(Long iat) {
        this.iat = iat;
    }

    public Long getExp() {
        return exp;
    }

    public void setExp(Long exp) {
        this.exp = exp;
    }
}
