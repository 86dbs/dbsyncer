/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.rocketmq.constant.RocketMQConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public final class RocketMQMessageUtil {

    private RocketMQMessageUtil() {
    }

    public static Map<String, Object> buildMessage(String tableName, String event, Map row) {
        Map<String, Object> message = new LinkedHashMap<>(4);
        message.put(RocketMQConstant.FIELD_TABLE, tableName);
        message.put(RocketMQConstant.FIELD_EVENT, event);
        message.put(RocketMQConstant.FIELD_DATA, row);
        return message;
    }

    public static String buildMessageKey(List<Field> pkFields, Map row) {
        if (CollectionUtils.isEmpty(pkFields) || row == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (Field field : pkFields) {
            Object value = row.get(field.getName());
            if (value != null) {
                parts.add(String.valueOf(value));
            }
        }
        return parts.isEmpty() ? null : StringUtil.join(parts, StringUtil.UNDERLINE);
    }

    public static ParsedMessage parse(Map<String, Object> valueMap) {
        if (valueMap == null) {
            return new ParsedMessage(null, ConnectorConstant.OPERTION_INSERT, new LinkedHashMap<>());
        }
        Object data = valueMap.get(RocketMQConstant.FIELD_DATA);
        if (data instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) data;
            String table = toStringValue(valueMap.get(RocketMQConstant.FIELD_TABLE));
            String event = toStringValue(valueMap.get(RocketMQConstant.FIELD_EVENT));
            if (StringUtil.isBlank(event)) {
                event = ConnectorConstant.OPERTION_INSERT;
            }
            return new ParsedMessage(table, event, row);
        }
        return new ParsedMessage(null, ConnectorConstant.OPERTION_INSERT, valueMap);
    }

    private static String toStringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    public static final class ParsedMessage {

        private final String table;
        private final String event;
        private final Map<String, Object> data;

        public ParsedMessage(String table, String event, Map<String, Object> data) {
            this.table = table;
            this.event = event;
            this.data = data;
        }

        public String getTable() {
            return table;
        }

        public String getEvent() {
            return event;
        }

        public Map<String, Object> getData() {
            return data;
        }
    }
}
