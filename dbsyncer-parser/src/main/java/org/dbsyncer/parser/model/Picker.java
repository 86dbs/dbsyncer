package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            final int size = source.size();
            final int sFieldSize = sourceFields.size();
            final int tFieldSize = targetFields.size();
            Map<String, Object> target = null;
            for (int i = 0; i < size; i++) {
                target = new HashMap<>();
                exchange(sFieldSize, tFieldSize, sourceFields, targetFields, source.get(i), target);
                targetMapList.add(target);
            }
        }
        return targetMapList;
    }

    public Map pickSourceData(Map target) {
        Map<String, Object> source = new HashMap<>();
        if (!CollectionUtils.isEmpty(target)) {
            exchange(targetFields.size(), sourceFields.size(), targetFields, sourceFields, target, source);
        }
        return source;
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
                String mergedValue = new StringBuilder(StringUtil.toString(target.get(tFieldName))).append(StringUtil.toString(v)).toString();
                target.put(tFieldName, mergedValue);
            }
        }
    }

    public List<Field> getTargetFields() {
        List<Field> fields = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        targetFields.stream().forEach(f -> {
            if (f != null && !keys.contains(f.getName())) {
                fields.add(f);
                keys.add(f.getName());
            }
        });
        keys.clear();
        return Collections.unmodifiableList(fields);
    }

    public Map<String, Field> getTargetFieldMap() {
        return targetFields.stream().filter(f -> null != f).collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
    }
}