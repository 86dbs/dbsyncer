package org.dbsyncer.connector.config;

import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.connector.model.Field;

public class DDLConfig {
    /**
     * 执行命令
     */
    private String sql;

    private AlterOperation operation;

    private List<Field> addFields;

    private List<Field> removeFields;

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

    public AlterOperation getOperation() {
        return operation;
    }

    public void setOperation(AlterOperation operation) {
        this.operation = operation;
    }
}
