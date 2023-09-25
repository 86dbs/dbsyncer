package org.dbsyncer.connector.config;

import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;

import java.util.LinkedList;
import java.util.List;

public class DDLConfig {
    /**
     * 执行命令
     */
    private String sql;

    private DDLOperationEnum ddlOperationEnum;

    private List<Field> addFields = new LinkedList<>();

    private List<Field> removeFields = new LinkedList<>();

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

    public List<Field> getAddFields() {
        return addFields;
    }

    public void setAddFields(List<Field> addFields) {
        this.addFields = addFields;
    }

    public List<Field> getRemoveFields() {
        return removeFields;
    }

    public void setRemoveFields(List<Field> removeFields) {
        this.removeFields = removeFields;
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