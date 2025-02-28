package org.dbsyncer.sdk.config;

import org.dbsyncer.sdk.enums.DDLOperationEnum;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DDLConfig {

    private String sql;

    private DDLOperationEnum ddlOperationEnum;

    private List<String> addedFieldNames = new LinkedList<>();

    private List<String> modifiedFieldNames = new LinkedList<>();

    private List<String> droppedFieldNames = new LinkedList<>();

    private Map<String, String> changedFieldNames = new LinkedHashMap<>();

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public DDLOperationEnum getDdlOperationEnum() {
        return ddlOperationEnum;
    }

    public void setDdlOperationEnum(DDLOperationEnum ddlOperationEnum) {
        this.ddlOperationEnum = ddlOperationEnum;
    }

    public List<String> getAddedFieldNames() {
        return addedFieldNames;
    }

    public void setAddedFieldNames(List<String> addedFieldNames) {
        this.addedFieldNames = addedFieldNames;
    }

    public List<String> getModifiedFieldNames() {
        return modifiedFieldNames;
    }

    public void setModifiedFieldNames(List<String> modifiedFieldNames) {
        this.modifiedFieldNames = modifiedFieldNames;
    }

    public List<String> getDroppedFieldNames() {
        return droppedFieldNames;
    }

    public void setDroppedFieldNames(List<String> droppedFieldNames) {
        this.droppedFieldNames = droppedFieldNames;
    }

    public Map<String, String> getChangedFieldNames() {
        return changedFieldNames;
    }

    public void setChangedFieldNames(Map<String, String> changedFieldNames) {
        this.changedFieldNames = changedFieldNames;
    }
}