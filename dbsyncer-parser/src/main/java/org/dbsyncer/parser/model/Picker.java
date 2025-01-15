package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.util.*;
import java.util.stream.Collectors;

public class Picker {

    private List<Field> sourceFields = new ArrayList<>();
    private List<Field> targetFields = new ArrayList<>();

    public Picker(List<FieldMapping> fieldMapping) {
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            fieldMapping.forEach(m -> {
                sourceFields.add(m.getSource());
                targetFields.add(m.getTarget());
            });
        }
    }

    public List<Map> pickTargetData(List<Map> source) {
        List<Map> targetMapList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(source)) {
            final int sFieldSize = sourceFields.size();
            final int tFieldSize = targetFields.size();
            Map<String, Object> target = null;
            for (Map row : source) {
                target = new HashMap<>();
                exchange(sFieldSize, tFieldSize, sourceFields, targetFields, row, target);
                targetMapList.add(target);
            }
        }
        return targetMapList;
    }

    public List<Map> pickTargetData(List<List<Object>> rows, List<Map> sourceMapList) {
        List<Map> targetMapList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(rows)) {
            final int sFieldSize = sourceFields.size();
            final int tFieldSize = targetFields.size();
            Map<String, Object> source = null;
            Map<String, Object> target = null;
            List<Object> row = null;
            List<Field> sFields = getFields(sourceFields);
            for (int i = 0; i < rows.size(); i++) {
                source = new HashMap<>();
                target = new HashMap<>();
                row = rows.get(i);
                for (int j = 0; j < row.size(); i++) {
                    source.put(sFields.get(j).getName(), row.get(j));
                }
                exchange(sFieldSize, tFieldSize, sourceFields, targetFields, source, target);
                sourceMapList.add(source);
                targetMapList.add(target);
            }
        }
        return targetMapList;
    }

    public List<Object> pickSourceData(Map target) {
        Map<String, Object> source = new HashMap<>();
        if (!CollectionUtils.isEmpty(target)) {
            exchange(targetFields.size(), sourceFields.size(), targetFields, sourceFields, target, source);
        }

        return getFields(sourceFields).stream().map(field -> source.get(field.getName())).collect(Collectors.toList());
    }

    public List<Field> getSourceFields() {
        return getFields(sourceFields);
    }

    public List<Field> getTargetFields() {
        return getFields(targetFields);
    }

    public Map<String, Field> getTargetFieldMap() {
        return targetFields.stream().filter(Objects::nonNull).collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
    }

    private void exchange(int sFieldSize, int tFieldSize, List<Field> sFields, List<Field> tFields, Map<String, Object> source, Map<String, Object> target) {
        Field sField = null;
        Field tField = null;
        Object v = null;
        String tFieldName = null;
        for (int k = 0; k < sFieldSize; k++) {
            sField = sFields.get(k);
            if (k < tFieldSize) {
                tField = tFields.get(k);
            }
            if (null != sField && null != tField) {
                v = source.get(sField.isUnmodifiabled() ? sField.getLabelName() : sField.getName());
                tFieldName = tField.getName();
                // 映射值
                if (!target.containsKey(tFieldName)) {
                    target.put(tFieldName, v);
                    continue;
                }
                // 合并值
                target.put(tFieldName, StringUtil.toString(target.get(tFieldName)) + StringUtil.toString(v));
            }
        }
    }

    private List<Field> getFields(List<Field> list) {
        List<Field> fields = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        list.forEach(f -> {
            if (f != null && !keys.contains(f.getName())) {
                fields.add(f);
                keys.add(f.getName());
            }
        });
        keys.clear();
        return Collections.unmodifiableList(fields);
    }

}