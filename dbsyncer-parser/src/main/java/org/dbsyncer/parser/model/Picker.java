package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Field;

import java.util.List;
import java.util.Map;

public class Picker {

    private List<Field> sourceFields;
    private List<Field> targetFields;
    private List<Map<String, Object>> targetList;
    private Map<String, Object> target;

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

    public List<Map<String, Object>> getTargetList() {
        return targetList;
    }

    public void setTargetList(List<Map<String, Object>> targetList) {
        this.targetList = targetList;
    }

    public Map<String, Object> getTarget() {
        return target;
    }

    public void setTarget(Map<String, Object> target) {
        this.target = target;
    }
}