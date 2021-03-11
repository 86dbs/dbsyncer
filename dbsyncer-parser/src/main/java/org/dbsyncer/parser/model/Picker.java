package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Field;

import java.util.List;
import java.util.Map;

public class Picker {

    private List<Field> sourceFields;
    private List<Field> targetFields;
    private List<Map> targetList;
    private Map target;

    public List<Field> getSourceFields() {
        return sourceFields;
    }

    public void setSourceFields(List<Field> sourceFields) {
        this.sourceFields = sourceFields;
    }

    public List<Field> getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(List<Field> targetFields) {
        this.targetFields = targetFields;
    }

    public List<Map> getTargetList() {
        return targetList;
    }

    public void setTargetList(List<Map> targetList) {
        this.targetList = targetList;
    }

    public Map getTarget() {
        return target;
    }

    public void setTarget(Map target) {
        this.target = target;
    }
}