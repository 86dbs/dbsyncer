/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.HeterogeneousDDLConverter;
import org.dbsyncer.sdk.config.DDLConfig;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * MySQL到SQL Server的DDL转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 20:30
 */
@Component
public class MySQLToSQLServerConverter implements HeterogeneousDDLConverter {

    @Override
    public String convert(String sourceConnectorType, String targetConnectorType, Alter alter, DDLConfig ddlConfig) {
        if (!"MySQL".equals(sourceConnectorType) || !"SqlServer".equals(targetConnectorType)) {
            return alter.toString();
        }

        // 获取表名
        String tableName = alter.getTable().getName();
        
        // 构建SQL Server的ALTER语句
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName);
        
        List<AlterExpression> expressions = alter.getAlterExpressions();
        for (int i = 0; i < expressions.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append(" ");
            
            AlterExpression expr = expressions.get(i);
            AlterOperation operation = expr.getOperation();
            
            switch (operation) {
                case ADD:
                    sql.append(convertAddExpression(expr));
                    break;
                case MODIFY:
                    sql.append(convertModifyExpression(expr));
                    break;
                case CHANGE:
                    sql.append(convertChangeExpression(expr));
                    break;
                case DROP:
                    sql.append(convertDropExpression(expr));
                    break;
                default:
                    // 对于不支持的操作，保持原样
                    sql.append(expr.toString());
                    break;
            }
        }
        
        return sql.toString();
    }

    private String convertAddExpression(AlterExpression expr) {
        StringBuilder result = new StringBuilder();
        result.append("ADD ");
        
        List<AlterExpression.ColumnDataType> columns = expr.getColDataTypeList();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            
            AlterExpression.ColumnDataType column = columns.get(i);
            String columnName = removeBackQuotes(column.getColumnName());
            String dataType = convertDataType(column.getColDataType());
            
            result.append(columnName).append(" ").append(dataType);
        }
        
        return result.toString();
    }

    private String convertModifyExpression(AlterExpression expr) {
        StringBuilder result = new StringBuilder();
        result.append("ALTER COLUMN ");
        
        List<AlterExpression.ColumnDataType> columns = expr.getColDataTypeList();
        if (!columns.isEmpty()) {
            AlterExpression.ColumnDataType column = columns.get(0);
            String columnName = removeBackQuotes(column.getColumnName());
            String dataType = convertDataType(column.getColDataType());
            
            result.append(columnName).append(" ").append(dataType);
        }
        
        return result.toString();
    }

    private String convertChangeExpression(AlterExpression expr) {
        // MySQL的CHANGE COLUMN在SQL Server中需要使用sp_rename存储过程
        // 这里简化处理，实际应该生成存储过程调用
        StringBuilder result = new StringBuilder();
        result.append("CHANGE COLUMN ");
        
        String oldName = removeBackQuotes(expr.getColumnOldName());
        List<AlterExpression.ColumnDataType> columns = expr.getColDataTypeList();
        if (!columns.isEmpty()) {
            AlterExpression.ColumnDataType column = columns.get(0);
            String newName = removeBackQuotes(column.getColumnName());
            String dataType = convertDataType(column.getColDataType());
            
            result.append(oldName).append(" ").append(newName).append(" ").append(dataType);
        }
        
        return result.toString();
    }

    private String convertDropExpression(AlterExpression expr) {
        StringBuilder result = new StringBuilder();
        result.append("DROP COLUMN ");
        
        String columnName = removeBackQuotes(expr.getColumnName());
        result.append(columnName);
        
        return result.toString();
    }

    private String convertDataType(ColDataType colDataType) {
        if (colDataType == null) {
            return "NVARCHAR(255)"; // 默认类型
        }
        
        String typeName = colDataType.getDataType();
        if (typeName == null) {
            return "NVARCHAR(255)"; // 默认类型
        }
        
        typeName = typeName.toUpperCase();
        
        // 简化的数据类型映射
        if (typeName.startsWith("VARCHAR") || typeName.startsWith("CHAR")) {
            // MySQL的VARCHAR转换为SQL Server的NVARCHAR
            return typeName.replace("VARCHAR", "NVARCHAR").replace("CHAR", "NCHAR");
        } else if (typeName.startsWith("DATETIME")) {
            // MySQL的DATETIME转换为SQL Server的DATETIME2
            return "DATETIME2";
        } else if (typeName.equals("TINYINT") && colDataType.getArgumentsStringList() != null 
                   && colDataType.getArgumentsStringList().size() > 0 
                   && "1".equals(colDataType.getArgumentsStringList().get(0))) {
            // MySQL的TINYINT(1)转换为SQL Server的BIT
            return "BIT";
        } else if (typeName.equals("INT")) {
            return "INT";
        } else if (typeName.equals("BIGINT")) {
            return "BIGINT";
        } else if (typeName.equals("SMALLINT")) {
            return "SMALLINT";
        } else if (typeName.equals("DECIMAL") || typeName.equals("NUMERIC")) {
            // 保持精度和小数位数
            if (colDataType.getArgumentsStringList() != null && !colDataType.getArgumentsStringList().isEmpty()) {
                StringBuilder result = new StringBuilder(typeName);
                result.append("(");
                for (int i = 0; i < colDataType.getArgumentsStringList().size(); i++) {
                    if (i > 0) {
                        result.append(",");
                    }
                    result.append(colDataType.getArgumentsStringList().get(i));
                }
                result.append(")");
                return result.toString();
            }
            return typeName;
        } else if (typeName.equals("FLOAT")) {
            return "REAL";
        } else if (typeName.equals("DOUBLE")) {
            return "FLOAT";
        } else if (typeName.equals("DATE")) {
            return "DATE";
        } else if (typeName.equals("TIME")) {
            return "TIME";
        } else if (typeName.equals("TEXT")) {
            return "NVARCHAR(MAX)";
        } else if (typeName.equals("BLOB")) {
            return "VARBINARY(MAX)";
        }
        
        // 其他类型默认转换为NVARCHAR(255)
        return "NVARCHAR(255)";
    }

    private String removeBackQuotes(String name) {
        if (name != null) {
            return StringUtil.replace(name, "`", "");
        }
        return name;
    }

    @Override
    public boolean supports(String sourceConnectorType, String targetConnectorType) {
        return "MySQL".equals(sourceConnectorType) && "SqlServer".equals(targetConnectorType);
    }
}