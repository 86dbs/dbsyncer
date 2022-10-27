package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.model.Field;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Picker {

    private List<Field> sourceFields;
    private List<Field> targetFields;

    public Picker(List<FieldMapping> fieldMapping) {
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
        List<Map> targetMapList = new ArrayList<>();
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

    public String getSourcePrimaryKeyName(AbstractConnectorConfig config) {
        for (Field f : sourceFields) {
            if (null != f && f.isPk()) {
                return f.getName();
            }
        }

        String primaryKey = config.getPrimaryKey();
        Assert.hasText(primaryKey, "主键为空");
        return primaryKey;
    }

    public List<Field> getSourceFields() {
        return sourceFields.stream().filter(f -> null != f).collect(Collectors.toList());
    }

    public List<Field> getTargetFields() {
        return targetFields.stream().filter(f -> null != f).collect(Collectors.toList());
    }

    public Map<String, Field> getSourceFieldMap() {
        return getSourceFields().stream().collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
    }
}