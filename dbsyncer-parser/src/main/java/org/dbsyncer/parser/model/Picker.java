package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.model.Field;

import java.util.*;

public class Picker {

    private List<Field> sourceFields;
    private List<Field> targetFields;
    private List<Map>   targetMapList;

    public Picker(List<FieldMapping> fieldMapping) {
        pickFields(fieldMapping);
    }

    public Picker(List<FieldMapping> fieldMapping, Map<String, Object> data) {
        pickFields(fieldMapping);
        pickData(data);
    }

    public void pickFields(List<FieldMapping> fieldMapping) {
        sourceFields = new ArrayList<>();
        targetFields = new ArrayList<>();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            fieldMapping.forEach(m -> {
                sourceFields.add(m.getSource());
                targetFields.add(m.getTarget());
            });
        }
    }

    public List<Map> pickData(List<Map> data) {
        targetMapList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(data)) {
            final int size = data.size();
            final int sFieldSize = sourceFields.size();
            Map<String, Object> target = null;
            for (int i = 0; i < size; i++) {
                target = new HashMap<>();
                exchange(sFieldSize, sourceFields, targetFields, data.get(i), target);
                targetMapList.add(target);
            }
        }
        return targetMapList;
    }

    public void pickData(Map<String, Object> data) {
        targetMapList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(data)) {
            Map targetMap = new HashMap<>();
            exchange(sourceFields.size(), sourceFields, targetFields, data, targetMap);
            targetMapList.add(targetMap);
        }
    }

    private void exchange(int sFieldSize, List<Field> sFields, List<Field> tFields, Map<String, Object> source,
                          Map<String, Object> target) {
        Field sField = null;
        Field tField = null;
        Object v = null;
        for (int k = 0; k < sFieldSize; k++) {
            sField = sFields.get(k);
            tField = tFields.get(k);
            if (null != sField && null != tField) {
                v = source.get(sField.isUnmodifiabled() ? sField.getLabelName() : sField.getName());
                target.put(tField.getName(), v);
            }
        }
    }

    public Map getTargetMap() {
        return CollectionUtils.isEmpty(targetMapList) ? Collections.EMPTY_MAP : targetMapList.get(0);
    }

    public List<Field> getTargetFields() {
        return targetFields;
    }

    public List<Map> getTargetMapList() {
        return targetMapList;
    }

}