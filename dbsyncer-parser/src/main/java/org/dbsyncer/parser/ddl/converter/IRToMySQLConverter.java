/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.converter;

import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.parser.ddl.ir.DDLOperationType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 中间表示到MySQL转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-27 10:50
 */
@Component
public class IRToMySQLConverter {

    public String convert(DDLIntermediateRepresentation ir) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(ir.getTableName());
        
        switch (ir.getOperationType()) {
            case ADD:
                sql.append(" ADD COLUMN ");
                sql.append(convertColumnsToAdd(ir.getColumns()));
                break;
            case MODIFY:
                sql.append(" MODIFY COLUMN ");
                sql.append(convertColumnsToModify(ir.getColumns()));
                break;
            case CHANGE:
                sql.append(" CHANGE COLUMN ");
                sql.append(convertColumnsToChange(ir.getColumns()));
                break;
            case DROP:
                sql.append(" DROP COLUMN ");
                sql.append(convertColumnsToDrop(ir.getColumns()));
                break;
        }
        return sql.toString();
    }
    
    private String convertColumnsToAdd(List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(column.getName()).append(" ").append(convertToMySQLType(column));
        }
        return result.toString();
    }
    
    private String convertColumnsToModify(List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(column.getName()).append(" ").append(convertToMySQLType(column));
        }
        return result.toString();
    }
    
    private String convertColumnsToChange(List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(column.getName()).append(" ").append(convertToMySQLType(column));
        }
        return result.toString();
    }
    
    private String convertColumnsToDrop(List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(column.getName());
        }
        return result.toString();
    }
    
    private String convertToMySQLType(Field column) {
        // 从类型名解析标准类型枚举
        DataTypeEnum standardType = DataTypeEnum.valueOf(column.getTypeName());
        if (standardType == null) {
            return "VARCHAR(255)"; // 默认类型
        }
        
        switch (standardType) {
            case STRING:
                if (column.getColumnSize() > 0) {
                    return "VARCHAR(" + column.getColumnSize() + ")";
                }
                return "VARCHAR(255)";
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case DECIMAL:
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return "DECIMAL(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return "DECIMAL(" + column.getColumnSize() + ")";
                }
                return "DECIMAL(10,0)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "DATETIME";
            case BOOLEAN:
                return "TINYINT(1)";
            case BYTES:
                return "LONGBLOB";
            default:
                return "VARCHAR(255)"; // 默认类型
        }
    }
}