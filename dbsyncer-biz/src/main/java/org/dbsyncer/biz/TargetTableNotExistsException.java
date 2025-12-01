package org.dbsyncer.biz;

/**
 * 目标表不存在异常
 * 用于标识目标表不存在，需要用户确认是否创建
 */
public class TargetTableNotExistsException extends BizException {

    /**
     * 错误码：用于前端识别异常类型
     */
    public static final String ERROR_CODE = "TARGET_TABLE_NOT_EXISTS";

    private String sourceConnectorId;
    private String targetConnectorId;
    private String sourceTable;
    private String targetTable;

    public TargetTableNotExistsException(String message, String sourceConnectorId,
                                         String targetConnectorId, String sourceTable, String targetTable) {
        super(message);
        this.sourceConnectorId = sourceConnectorId;
        this.targetConnectorId = targetConnectorId;
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
    }

    public String getErrorCode() {
        return ERROR_CODE;
    }

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }
}

