/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.util.Map;

/**
 * 同步任务列表：任务进度摘要。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29
 */
public class MappingListMetaVO {

    private int state;
    private Long updateTime;
    private long startTime;
    private long total;
    private long success;
    private long fail;
    private boolean counting;
    /**
     * 仅保留列表展示阶段所需键。
     */
    private Map<String, String> snapshot;

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getFail() {
        return fail;
    }

    public void setFail(long fail) {
        this.fail = fail;
    }

    public boolean isCounting() {
        return counting;
    }

    public void setCounting(boolean counting) {
        this.counting = counting;
    }

    public Map<String, String> getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
    }
}
