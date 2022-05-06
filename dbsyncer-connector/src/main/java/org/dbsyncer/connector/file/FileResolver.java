package org.dbsyncer.connector.file;

import org.dbsyncer.connector.file.column.ColumnValue;
import org.dbsyncer.connector.file.column.FileColumnValue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 15:46
 */
public class FileResolver {

    private ColumnValue value = new FileColumnValue();

    /**
     * Resolve the value of a {@link ColumnValue}.
     *
     * @param typeName
     * @param columnValue
     * @return
     */
    protected Object resolveValue(String typeName, String columnValue) {
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

}
