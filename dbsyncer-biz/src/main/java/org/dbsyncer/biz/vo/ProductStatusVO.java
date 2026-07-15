/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 授权产品状态
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-07-12 22:20
 */
public class ProductStatusVO {

    private String name;
    private String status;
    private long effectiveTime;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getEffectiveTime() {
        return effectiveTime;
    }

    public void setEffectiveTime(long effectiveTime) {
        this.effectiveTime = effectiveTime;
    }
}
