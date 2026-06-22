/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

/**
 * 订正校验单表终态结果（落库字段与 {@link org.dbsyncer.sdk.enums.StorageEnum#VALIDATE_SYNC_DETAIL} 列对应）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-04 18:30
 */
public class ValidateSyncDetailResult {

    /** 明细类型，如 rowData、tableSchema */
    private String detailType;
    private String sourceTableName;
    private String targetTableName;
    private Long sourceTotal;
    private Long targetTotal;
    private long diffTotal;
    private long fixedTotal;
    /** 差异 data 列表 JSON，汇总指标走独立列 */
    private String content;

    public static ValidateSyncDetailResult of(String detailType) {
        ValidateSyncDetailResult result = new ValidateSyncDetailResult();
        result.detailType = detailType;
        return result;
    }

    public ValidateSyncDetailResult tables(String sourceTableName, String targetTableName) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        return this;
    }

    public ValidateSyncDetailResult sourceTotal(Long sourceTotal) {
        this.sourceTotal = sourceTotal;
        return this;
    }

    public ValidateSyncDetailResult targetTotal(Long targetTotal) {
        this.targetTotal = targetTotal;
        return this;
    }

    public ValidateSyncDetailResult diffTotal(long diffTotal) {
        this.diffTotal = diffTotal;
        return this;
    }

    public ValidateSyncDetailResult fixedTotal(long fixedTotal) {
        this.fixedTotal = fixedTotal;
        return this;
    }

    public ValidateSyncDetailResult content(String content) {
        this.content = content;
        return this;
    }

    public String getDetailType() {
        return detailType;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public Long getSourceTotal() {
        return sourceTotal;
    }

    public Long getTargetTotal() {
        return targetTotal;
    }

    public long getDiffTotal() {
        return diffTotal;
    }

    public long getFixedTotal() {
        return fixedTotal;
    }

    public String getContent() {
        return content;
    }
}
