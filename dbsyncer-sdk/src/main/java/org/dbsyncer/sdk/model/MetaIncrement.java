/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.constant.ConfigConstant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Meta 计数原子增量参数（可为负数）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-24 10:42
 */
public class MetaIncrement {

    /** 元数据ID */
    private String metaId;
    /** 总量增量 */
    private long totalDelta;
    /** 成功数量增量 */
    private long successDelta;
    /** 失败数量增量 */
    private long failDelta;
    /** 差异数量增量 */
    private long diffDelta;
    /** 修复数量增量 */
    private long fixedDelta;

    public static MetaIncrement of(String metaId) {
        MetaIncrement increment = new MetaIncrement();
        increment.metaId = metaId;
        return increment;
    }

    public MetaIncrement total(long totalDelta) {
        this.totalDelta = totalDelta;
        return this;
    }

    public MetaIncrement success(long successDelta) {
        this.successDelta = successDelta;
        return this;
    }

    public MetaIncrement fail(long failDelta) {
        this.failDelta = failDelta;
        return this;
    }

    public MetaIncrement diff(long diffDelta) {
        this.diffDelta = diffDelta;
        return this;
    }

    public MetaIncrement fixed(long fixedDelta) {
        this.fixedDelta = fixedDelta;
        return this;
    }

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public long getTotalDelta() {
        return totalDelta;
    }

    public void setTotalDelta(long totalDelta) {
        this.totalDelta = totalDelta;
    }

    public long getSuccessDelta() {
        return successDelta;
    }

    public void setSuccessDelta(long successDelta) {
        this.successDelta = successDelta;
    }

    public long getFailDelta() {
        return failDelta;
    }

    public void setFailDelta(long failDelta) {
        this.failDelta = failDelta;
    }

    public long getDiffDelta() {
        return diffDelta;
    }

    public void setDiffDelta(long diffDelta) {
        this.diffDelta = diffDelta;
    }

    public long getFixedDelta() {
        return fixedDelta;
    }

    public void setFixedDelta(long fixedDelta) {
        this.fixedDelta = fixedDelta;
    }

     /**
     * 转为存储增量 Map：key 与 {@link ConfigConstant} Meta 列名一致（total/success/fail/diff/fixed），值为 0 的项不入表。
     *
     * @return 非空增量；全为 0 时返回空 Map
     */
    public Map<String, Long> toDeltaMap() {
        Map<String, Long> deltas = new HashMap<>(5);
        putIfNonZero(deltas, ConfigConstant.META_TOTAL, totalDelta);
        putIfNonZero(deltas, ConfigConstant.META_SUCCESS, successDelta);
        putIfNonZero(deltas, ConfigConstant.META_FAIL, failDelta);
        putIfNonZero(deltas, ConfigConstant.META_DIFF, diffDelta);
        putIfNonZero(deltas, ConfigConstant.META_FIXED, fixedDelta);
        return deltas.isEmpty() ? Collections.emptyMap() : deltas;
    }

    private static void putIfNonZero(Map<String, Long> deltas, String key, long value) {
        if (value != 0L) {
            deltas.put(key, value);
        }
    }
}
