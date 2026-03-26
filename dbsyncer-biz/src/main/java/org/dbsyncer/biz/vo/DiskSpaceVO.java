/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.math.BigDecimal;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-20 20:50
 */
public final class DiskSpaceVO {

    private BigDecimal used;
    private BigDecimal free;
    private BigDecimal total;
    private BigDecimal usedPercent;

    public BigDecimal getUsed() {
        return used;
    }

    public void setUsed(BigDecimal used) {
        this.used = used;
    }

    public BigDecimal getFree() {
        return free;
    }

    public void setFree(BigDecimal free) {
        this.free = free;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    public BigDecimal getUsedPercent() {
        return usedPercent;
    }

    public void setUsedPercent(BigDecimal usedPercent) {
        this.usedPercent = usedPercent;
    }
}
