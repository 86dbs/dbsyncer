/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 同步任务 Meta 日汇总（{@code dbsyncer_meta_agg}），供首页同步趋势使用。
 *
 * @author wuji
 * @version 1.0.0
 */
public class MetaAgg extends ConfigModel {

    /**
     * 统计日期 yyyy-MM-dd
     */
    private String statDate;

    /**
     * 同步任务 Meta.total 合计
     */
    private long total;

    /**
     * 同步任务 Meta.success 合计
     */
    private long success;

    /**
     * 同步任务 Meta.fail 合计
     */
    private long fail;

    public MetaAgg() {
        super.setName("");
        super.setType(ConfigConstant.META_DAILY);
    }

    public String getStatDate() {
        return statDate;
    }

    public void setStatDate(String statDate) {
        this.statDate = statDate;
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
}
