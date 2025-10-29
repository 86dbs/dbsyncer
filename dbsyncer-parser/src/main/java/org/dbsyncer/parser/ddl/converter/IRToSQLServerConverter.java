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
 * 中间表示到SQL Server转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-27 10:45
 */
@Component
public class IRToSQLServerConverter {

    public String convert(DDLIntermediateRepresentation ir) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(ir.getTableName());
        
        switch (ir.getOperationType()) {
            case ADD:
                sql.append(" ADD ");
                sql.append(convertColumnsToAdd(ir.getColumns()));
                break;
            case MODIFY:
                sql.append(" ALTER COLUMN ");
                sql.append(convertColumnsToModify(ir.getColumns()));
                break;
            case CHANGE:
                // SQL Server使用sp_rename存储过程进行重命名
                // 这里简化处理，实际应该生成存储过程调用
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
            result.append(column.getName()).append(" ").append(convertToSQLServerType(column));
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
            result.append(column.getName()).append(" ").append(convertToSQLServerType(column));
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
            result.append(column.getName()).append(" ").append(convertToSQLServerType(column));
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
    
    private String convertToSQLServerType(Field column) {
        // 从类型名解析标准类型枚举
        DataTypeEnum standardType = DataTypeEnum.valueOf(column.getTypeName());
        if (standardType == null) {
            return "NVARCHAR(255)"; // 默认类型
        }
        
        switch (standardType) {
            case STRING:
                if (column.getColumnSize() > 0) {
                    return "NVARCHAR(" + column.getColumnSize() + ")";
                }
                return "NVARCHAR(255)";
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
                return "DECIMAL(18,0)";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "DATETIME2";
            case BOOLEAN:
                return "BIT";
            case BYTES:
                return "VARBINARY(MAX)";
            default:
                return "NVARCHAR(255)"; // 默认类型
        }
    }
}