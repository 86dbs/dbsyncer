/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

/**
 * Token信息
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-07 00:02
 */
public class TokenInfo {

    /**
     * 签发时间
     */
    private Long iat;

    /**
     * 有效期
     */
    private Long exp;

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
