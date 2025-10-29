/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.converter;

import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 中间表示到SQL Server转换器
 */
@Component
public class IRToSQLServerConverter {

    private final SqlServerTemplate sqlServerTemplate = new SqlServerTemplate();

    public String convert(DDLIntermediateRepresentation ir) {
        StringBuilder sql = new StringBuilder();

        switch (ir.getOperationType()) {
            case ADD:
                sql.append(convertColumnsToAdd(ir.getTableName(), ir.getColumns()));
                break;
            case MODIFY:
                sql.append(convertColumnsToModify(ir.getTableName(), ir.getColumns()));
                break;
            case CHANGE:
                sql.append(convertColumnsToChange(ir.getTableName(), ir.getColumns()));
                break;
            case DROP:
                sql.append(convertColumnsToDrop(ir.getTableName(), ir.getColumns()));
                break;
        }
        return sql.toString();
    }

    private String convertColumnsToAdd(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append("ALTER TABLE ").append(tableName).append(" ADD ")
                    .append(column.getName()).append(" ").append(sqlServerTemplate.convertToSQLServerType(column));
        }
        return result.toString();
    }

    private String convertColumnsToModify(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append("ALTER TABLE ").append(tableName).append(" ALTER COLUMN ")
                    .append(column.getName()).append(" ").append(sqlServerTemplate.convertToSQLServerType(column));
        }
        return result.toString();
    }

    private String convertColumnsToChange(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        // SQL Server使用sp_rename存储过程进行重命名
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append("EXEC sp_rename '").append(tableName).append(".").append(column.getName())
                    .append("', '").append(column.getName()).append("', 'COLUMN'");
        }
        return result.toString();
    }

    private String convertColumnsToDrop(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append("ALTER TABLE ").append(tableName).append(" DROP COLUMN ").append(column.getName());
        }
        return result.toString();
    }
}