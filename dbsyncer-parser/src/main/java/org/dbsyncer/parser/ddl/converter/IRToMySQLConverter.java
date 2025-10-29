/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.converter;

import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 中间表示到MySQL转换器
 */
@Component
public class IRToMySQLConverter implements IRToTargetConverter {

    private final MySQLTemplate mysqlTemplate = new MySQLTemplate();

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
            result.append(mysqlTemplate.buildAddColumnSql(tableName, column));
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
            result.append(mysqlTemplate.buildModifyColumnSql(tableName, column));
        }
        return result.toString();
    }

    private String convertColumnsToChange(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            // 对于CHANGE操作，我们假设字段名不变，仅类型改变
            result.append(mysqlTemplate.buildRenameColumnSql(tableName, column.getName(), column));
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
            result.append(mysqlTemplate.buildDropColumnSql(tableName, column.getName()));
        }
        return result.toString();
    }
}