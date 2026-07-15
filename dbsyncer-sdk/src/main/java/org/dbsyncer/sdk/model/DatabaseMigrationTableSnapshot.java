/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.io.Serializable;

/**
 * 表级迁移快照（按表映射 index 索引）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:30
 */
public class DatabaseMigrationTableSnapshot extends CommonTaskSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private long successTotal;

    private long failTotal;

    public long getSuccessTotal() {
        return successTotal;
    }

    public void setSuccessTotal(long successTotal) {
        this.successTotal = successTotal;
    }

    public long getFailTotal() {
        return failTotal;
    }

    public void setFailTotal(long failTotal) {
        this.failTotal = failTotal;
    }
}
