/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.model;

import org.dbsyncer.common.column.Lexer;
import org.dbsyncer.connector.file.column.ColumnValue;
import org.dbsyncer.connector.file.column.impl.FileColumnValue;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-06 00:04
 */
public class FileResolver {

    private ColumnValue value = new FileColumnValue();

    public Map<String, Object> parseMap(List<Field> fields, char separator, String line) {
        Map<String, Object> row = new LinkedHashMap<>();
        parse(fields, separator, line, (key, value) -> row.put(key, value));
        return row;
    }

    public List<Object> parseList(List<Field> fields, char separator, String line) {
        List<Object> data = new ArrayList<>();
        parse(fields, separator, line, (key, value) -> data.add(value));
        return data;
    }

    /**
     * Resolve the value of a {@link ColumnValue}.
     *
     * @param typeName
     * @param columnValue
     * @return
     */
    private Object resolveValue(String typeName, String columnValue) {
        value.setValue(columnValue);

        if (value.isNull()) {
            return null;
        }

        switch (typeName) {
            case "string":
                return value.asString();

            case "integer":
                return value.asInteger();

            case "date":
                return value.asDate();

            case "timestamp":
                return value.asTimestamp();

            case "boolean":
                return value.asBoolean();

            case "long":
                return value.asLong();

            case "float":
                return value.asFloat();

            case "double":
                return value.asDouble();

            case "time":
                return value.asTime();

            case "bytea":
                return value.asByteArray();

            default:
                return null;
        }

    }

    private void parse(List<Field> fields, char separator, String line, ResultSetMapper mapper) {
        int fieldSize = fields.size();
        int i = 0;
        Lexer lexer = new Lexer(line);
        while (i < fieldSize) {
            if (lexer.hasNext()) {
                mapper.apply(fields.get(i).getName(), resolveValue(fields.get(i).getTypeName(), lexer.nextToken(separator)));
            } else {
                mapper.apply(fields.get(i).getName(), null);
            }
            i++;
        }
    }

    private interface ResultSetMapper {
        void apply(String key, Object value);
    }

}
