/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.cdc;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 解析 OceanBase LogProxy {@link LogMessage}
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-05 00:20
 */
public final class OceanBaseLogMessageParser {

    private OceanBaseLogMessageParser() {
    }

    /**
     * 从 LogMessage 的数据库名解析业务库名（格式 tenant.db）
     */
    public static String parseDatabaseName(String dbName) {
        if (StringUtil.isBlank(dbName)) {
            return dbName;
        }
        int idx = dbName.indexOf('.');
        return idx >= 0 ? dbName.substring(idx + 1) : dbName;
    }

    public static Map<String, Object> toValueMap(LogMessage message, boolean before) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (message == null || message.getFieldList() == null) {
            return values;
        }
        for (DataMessage.Record.Field field : message.getFieldList()) {
            if (before != field.isPrev()) {
                continue;
            }
            values.put(field.getFieldname(), readFieldValue(field));
        }
        return values;
    }

    public static List<Object> toRowList(List<Field> columns, Map<String, Object> valueMap) {
        if (columns == null || valueMap == null) {
            return columns == null ? null : columns.stream().map(c -> (Object) null).collect(Collectors.toList());
        }
        Map<String, Object> lowerCaseMap = new HashMap<>(valueMap.size());
        valueMap.forEach((k, v) -> lowerCaseMap.put(k.toLowerCase(), v));
        return columns.stream()
                .map(c -> {
                    Object val = valueMap.get(c.getName());
                    if (val == null) {
                        val = lowerCaseMap.get(c.getName().toLowerCase());
                    }
                    return val;
                })
                .collect(Collectors.toList());
    }

    public static String readDdlSql(LogMessage message) {
        if (message == null || message.getFieldList() == null || message.getFieldList().isEmpty()) {
            return null;
        }
        Object value = readFieldValue(message.getFieldList().get(0));
        return value == null ? null : value.toString();
    }

    private static Object readFieldValue(DataMessage.Record.Field field) {
        if (field == null || field.getValue() == null) {
            return null;
        }
        if ("binary".equalsIgnoreCase(field.getEncoding())) {
            return field.getValue().toString("utf8");
        }
        return field.getValue().toString(field.getEncoding());
    }
}
