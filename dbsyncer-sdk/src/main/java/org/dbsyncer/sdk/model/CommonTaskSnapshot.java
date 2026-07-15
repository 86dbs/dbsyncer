/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.enums.CommonTaskStepStatusEnum;
import org.dbsyncer.sdk.enums.DatabaseMigrationDetailTypeEnum;

import java.io.Serializable;

/**
 * 通用任务表级快照：step 在迁移场景为 {@link DatabaseMigrationDetailTypeEnum}，
 * 在校验场景为 {@link org.dbsyncer.sdk.enums.ValidateSyncStepEnum}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-09 10:37
 */
public class CommonTaskSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 当前阶段编码 */
    private String step;

    /** 当前阶段状态，见 {@link CommonTaskStepStatusEnum} */
    private int status;

    /** 数据迁移分页游标 */
    private String cursor;

    private long pageIndex;

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public long getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(long pageIndex) {
        this.pageIndex = pageIndex;
    }
}
