/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.parser.ddl.ir.DDLOperationType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL到中间表示转换器
 */
@Component
public class MySQLToIRConverter implements SourceToIRConverter {

    @Override
    public DDLIntermediateRepresentation convert(Alter alter) {
        DDLIntermediateRepresentation ir = new DDLIntermediateRepresentation();
        ir.setTableName(alter.getTable().getName());
        
        for (AlterExpression expr : alter.getAlterExpressions()) {
            switch (expr.getOperation()) {
                case ADD:
                    ir.setOperationType(DDLOperationType.ADD);
                    ir.setColumns(convertColumns(expr.getColDataTypeList()));
                    break;
                case MODIFY:
                    ir.setOperationType(DDLOperationType.MODIFY);
                    ir.setColumns(convertColumns(expr.getColDataTypeList()));
                    break;
                case CHANGE:
                    ir.setOperationType(DDLOperationType.CHANGE);
                    ir.setColumns(convertColumns(expr.getColDataTypeList()));
                    break;
                case DROP:
                    ir.setOperationType(DDLOperationType.DROP);
                    List<Field> dropColumns = new ArrayList<>();
                    Field dropColumn = new Field();
                    dropColumn.setName(removeBackQuotes(expr.getColumnName()));
                    dropColumns.add(dropColumn);
                    ir.setColumns(dropColumns);
                    break;
            }
        }
        
        return ir;
    }
    
    private List<Field> convertColumns(List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<Field> columns = new ArrayList<>();
        if (columnDataTypes != null) {
            for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
                Field column = new Field();
                column.setName(removeBackQuotes(columnDataType.getColumnName()));
                ColDataType colDataType = columnDataType.getColDataType();
                if (colDataType != null) {
                    // 设置标准类型名和类型编码
                    DataTypeEnum standardType = convertToStandardType(colDataType.getDataType());
                    column.setTypeName(standardType.name());
                    column.setType(standardType.ordinal());
                    
                    // 处理长度和精度
                    List<String> args = colDataType.getArgumentsStringList();
                    if (args != null && !args.isEmpty()) {
                        if (args.size() >= 1) {
                            try {
                                column.setColumnSize(Integer.parseInt(args.get(0)));
                            } catch (NumberFormatException e) {
                                // 忽略解析错误
                            }
                        }
                        if (args.size() >= 2) {
                            try {
                                column.setRatio(Integer.parseInt(args.get(1)));
                            } catch (NumberFormatException e) {
                                // 忽略解析错误
                            }
                        }
                    }
                }
                columns.add(column);
            }
        }
        return columns;
    }
    
    private DataTypeEnum convertToStandardType(String mysqlDataType) {
        if (mysqlDataType == null) {
            return DataTypeEnum.STRING; // 默认类型
        }
        
        String type = mysqlDataType.toUpperCase();
        if (type.startsWith("VARCHAR") || type.startsWith("CHAR")) {
            return DataTypeEnum.STRING;
        } else if ("INT".equals(type) || "INTEGER".equals(type)) {
            return DataTypeEnum.INT;
        } else if ("BIGINT".equals(type)) {
            return DataTypeEnum.LONG;
        } else if ("DECIMAL".equals(type) || "NUMERIC".equals(type)) {
            return DataTypeEnum.DECIMAL;
        } else if ("FLOAT".equals(type)) {
            return DataTypeEnum.FLOAT;
        } else if ("DOUBLE".equals(type)) {
            return DataTypeEnum.DOUBLE;
        } else if ("DATE".equals(type)) {
            return DataTypeEnum.DATE;
        } else if ("TIME".equals(type)) {
            return DataTypeEnum.TIME;
        } else if (type.startsWith("DATETIME") || "TIMESTAMP".equals(type)) {
            return DataTypeEnum.TIMESTAMP;
        } else if ("TINYINT".equals(type) && type.contains("1")) {
            return DataTypeEnum.BOOLEAN;
        } else if ("TINYINT".equals(type) || "SMALLINT".equals(type) || "MEDIUMINT".equals(type)) {
            return DataTypeEnum.INT;
        } else if ("TEXT".equals(type) || "TINYTEXT".equals(type) || "MEDIUMTEXT".equals(type) || "LONGTEXT".equals(type)) {
            return DataTypeEnum.STRING;
        } else if ("BLOB".equals(type) || "TINYBLOB".equals(type) || "MEDIUMBLOB".equals(type) || "LONGBLOB".equals(type)) {
            return DataTypeEnum.BYTES;
        }
        
        return DataTypeEnum.STRING; // 默认返回字符串类型
    }
    
    private String removeBackQuotes(String name) {
        if (name != null) {
            return StringUtil.replace(name, "`", "");
        }
        return name;
    }
}