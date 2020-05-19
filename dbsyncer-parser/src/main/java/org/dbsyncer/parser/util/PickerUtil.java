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
        if(!CollectionUtils.isEmpty(fieldMapping)){
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
        if(!CollectionUtils.isEmpty(data)){
            List<Map<String, Object>> target = new ArrayList<>();
            List<Field> sFields = picker.getSourceFields();
            List<Field> tFields = picker.getTargetFields();

            final int kSize = sFields.size();
            final int size = data.size();
            Map<String, Object> row = null;
            Map<String, Object> r = null;
            String sName = null;
            String tName = null;
            Object v = null;
            for (int i = 0; i < size; i++) {
                row = data.get(i);
                r = new HashMap<>();

                for (int k = 0; k < kSize; k++) {
                    sName = sFields.get(k).getName();
                    v = row.get(sName);

                    tName = tFields.get(k).getName();
                    r.put(tName, v);
                }
                target.add(r);
            }

            picker.setTargetList(target);
        }
    }

    public static void pickData(Picker picker, Map<String, Object> row) {
        if(!CollectionUtils.isEmpty(row)){
            Map<String, Object> target = new HashMap<>();
            List<Field> sFields = picker.getSourceFields();
            List<Field> tFields = picker.getTargetFields();

            final int kSize = sFields.size();
            String sName = null;
            String tName = null;
            Object v = null;
            for (int k = 0; k < kSize; k++) {
                sName = sFields.get(k).getName();
                v = row.get(sName);

                tName = tFields.get(k).getName();
                target.put(tName, v);
            }

            picker.setTarget(target);
        }
    }

}