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
 * SQL Server到MySQL的DDL转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 20:45
 */
@Component
public class SQLServerToMySQLConverter implements HeterogeneousDDLConverter {

    @Override
    public String convert(String sourceConnectorType, String targetConnectorType, Alter alter, DDLConfig ddlConfig) {
        if (!"SqlServer".equals(sourceConnectorType) || !"MySQL".equals(targetConnectorType)) {
            return alter.toString();
        }

        // 获取表名
        String tableName = alter.getTable().getName();
        
        // 构建MySQL的ALTER语句
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
                case ALTER:
                    sql.append(convertAlterExpression(expr));
                    break;
                // SQL Server没有CHANGE操作，重命名需要特殊处理
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
        result.append("ADD COLUMN ");
        
        List<AlterExpression.ColumnDataType> columns = expr.getColDataTypeList();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            
            AlterExpression.ColumnDataType column = columns.get(i);
            String columnName = removeBrackets(column.getColumnName());
            String dataType = convertDataType(column.getColDataType());
            
            result.append(columnName).append(" ").append(dataType);
        }
        
        return result.toString();
    }

    private String convertAlterExpression(AlterExpression expr) {
        StringBuilder result = new StringBuilder();
        result.append("MODIFY COLUMN ");
        
        List<AlterExpression.ColumnDataType> columns = expr.getColDataTypeList();
        if (!columns.isEmpty()) {
            AlterExpression.ColumnDataType column = columns.get(0);
            String columnName = removeBrackets(column.getColumnName());
            String dataType = convertDataType(column.getColDataType());
            
            result.append(columnName).append(" ").append(dataType);
        }
        
        return result.toString();
    }

    private String convertDropExpression(AlterExpression expr) {
        StringBuilder result = new StringBuilder();
        result.append("DROP COLUMN ");
        
        String columnName = removeBrackets(expr.getColumnName());
        result.append(columnName);
        
        return result.toString();
    }

    private String convertDataType(ColDataType colDataType) {
        if (colDataType == null) {
            return "VARCHAR(255)"; // 默认类型
        }
        
        String typeName = colDataType.getDataType();
        if (typeName == null) {
            return "VARCHAR(255)"; // 默认类型
        }
        
        typeName = typeName.toUpperCase();
        
        // 简化的数据类型映射
        if (typeName.startsWith("NVARCHAR") || typeName.startsWith("NCHAR")) {
            // SQL Server的NVARCHAR转换为MySQL的VARCHAR
            return typeName.replace("NVARCHAR", "VARCHAR").replace("NCHAR", "CHAR");
        } else if (typeName.startsWith("DATETIME2")) {
            // SQL Server的DATETIME2转换为MySQL的DATETIME
            return "DATETIME";
        } else if (typeName.equals("BIT")) {
            // SQL Server的BIT转换为MySQL的TINYINT(1)
            return "TINYINT(1)";
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
        } else if (typeName.equals("REAL")) {
            return "FLOAT";
        } else if (typeName.equals("FLOAT")) {
            return "DOUBLE";
        } else if (typeName.equals("DATE")) {
            return "DATE";
        } else if (typeName.equals("TIME")) {
            return "TIME";
        } else if (typeName.equals("NVARCHAR(MAX)") || typeName.equals("NTEXT")) {
            return "TEXT";
        } else if (typeName.equals("VARBINARY(MAX)") || typeName.equals("IMAGE")) {
            return "LONGBLOB";
        }
        
        // 其他类型默认转换为VARCHAR(255)
        return "VARCHAR(255)";
    }

    private String removeBrackets(String name) {
        if (name != null) {
            name = StringUtil.replace(name, "[", "");
            name = StringUtil.replace(name, "]", "");
            return name;
        }
        return name;
    }

    @Override
    public boolean supports(String sourceConnectorType, String targetConnectorType) {
        return "SqlServer".equals(sourceConnectorType) && "MySQL".equals(targetConnectorType);
    }
}