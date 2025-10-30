/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.converter;

import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.IRToTargetConverter;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;

import java.util.List;

/**
 * 中间表示到SQL Server转换器
 */
public class IRToSQLServerConverter implements IRToTargetConverter {

    private final SqlServerTemplate sqlServerTemplate = new SqlServerTemplate();

    @Override
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
            result.append(sqlServerTemplate.buildAddColumnSql(tableName, column));
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
            result.append(sqlServerTemplate.buildModifyColumnSql(tableName, column));
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
            // 对于CHANGE操作，我们假设字段名不变，仅类型改变
            // 实际应用中可能需要更复杂的处理
            result.append(sqlServerTemplate.buildRenameColumnSql(tableName, column.getName(), column));
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
            result.append(sqlServerTemplate.buildDropColumnSql(tableName, column.getName()));
        }
        return result.toString();
    }
}