package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.enums.DDLOperationEnum;

import java.util.LinkedList;
import java.util.List;

public class DDLConfig {
    /**
     * 执行命令
     */
    private String sql;

    private DDLOperationEnum ddlOperationEnum;

    private List<String> addFieldNames = new LinkedList<>();

    private List<String> removeFieldNames = new LinkedList<>();

    //记录源表的源字段名称
    private String sourceColumnName;

    //记录改变后的字段名称
    private String changedColumnName;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<String> getAddFieldNames() {
        return addFieldNames;
    }

    public void setAddFieldNames(List<String> addFieldNames) {
        this.addFieldNames = addFieldNames;
    }

    public List<String> getRemoveFieldNames() {
        return removeFieldNames;
    }

    public void setRemoveFieldNames(List<String> removeFieldNames) {
        this.removeFieldNames = removeFieldNames;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    public String getChangedColumnName() {
        return changedColumnName;
    }

    public void setChangedColumnName(String changedColumnName) {
        this.changedColumnName = changedColumnName;
    }

    public DDLOperationEnum getDdlOperationEnum() {
        return ddlOperationEnum;
    }

    public void setDdlOperationEnum(DDLOperationEnum ddlOperationEnum) {
        this.ddlOperationEnum = ddlOperationEnum;
    }
}