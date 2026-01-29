/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 同步趋势数据
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-20 02:33
 */
public class SyncTrendStackVO {

    private List<String> labels;

    private List<Long> success;

    private List<Long> fail;

    public SyncTrendStackVO() {
        this.labels = new CopyOnWriteArrayList<>();
        this.success = new CopyOnWriteArrayList<>();
        this.fail = new CopyOnWriteArrayList<>();
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public List<Long> getSuccess() {
        return success;
    }

    public void setSuccess(List<Long> success) {
        this.success = success;
    }

    public List<Long> getFail() {
        return fail;
    }

    public void setFail(List<Long> fail) {
        this.fail = fail;
    }
}