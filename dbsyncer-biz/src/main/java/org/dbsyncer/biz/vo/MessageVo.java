package org.dbsyncer.biz.vo;

import java.util.List;

public class MessageVo {

    private String id;

    private String sourceTableName;

    private String targetTableName;

    private List<BinlogColumnVo> columns;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public List<BinlogColumnVo> getColumns() {
        return columns;
    }

    public void setColumns(List<BinlogColumnVo> columns) {
        this.columns = columns;
    }
}
