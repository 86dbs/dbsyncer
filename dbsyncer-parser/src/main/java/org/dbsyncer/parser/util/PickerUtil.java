package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Picker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class PickerUtil {

    private PickerUtil() {
    }

    public static void pickFields(Picker picker, List<FieldMapping> fieldMapping) {
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            List<Field> sFields = new ArrayList<>();
            List<Field> tFields = new ArrayList<>();
            fieldMapping.forEach(m -> {
                sFields.add(m.getSource());
                tFields.add(m.getTarget());
            });
            picker.setSourceFields(sFields);
            picker.setTargetFields(tFields);
        }
    }

    public static void pickData(Picker picker, List<Map<String, Object>> data) {
        if (!CollectionUtils.isEmpty(data)) {
            List<Map<String, Object>> target = new ArrayList<>();
            List<Field> sFields = picker.getSourceFields();
            List<Field> tFields = picker.getTargetFields();

            final int size = data.size();
            final int sFieldSize = sFields.size();
            Map<String, Object> row = null;
            Map<String, Object> r = null;
            for (int i = 0; i < size; i++) {
                row = data.get(i);
                r = new HashMap<>();
                exchange(sFieldSize, sFields, tFields, row, r);
                target.add(r);
            }

            picker.setTargetList(target);
        }
    }

    public static void pickData(Picker picker, Map<String, Object> row) {
        if (!CollectionUtils.isEmpty(row)) {
            Map<String, Object> target = new HashMap<>();
            List<Field> sFields = picker.getSourceFields();
            List<Field> tFields = picker.getTargetFields();

            exchange(sFields.size(), sFields, tFields, row, target);
            picker.setTarget(target);
        }
    }

    private static void exchange(int sFieldSize, List<Field> sFields, List<Field> tFields, Map<String, Object> source, Map<String, Object> target) {
        Field sField = null;
        Object v = null;
        for (int k = 0; k < sFieldSize; k++) {
            sField = sFields.get(k);
            if (null != sField) {
                v = source.get(sField.getName());

                target.put(tFields.get(k).getName(), v);
            }
        }
    }

}