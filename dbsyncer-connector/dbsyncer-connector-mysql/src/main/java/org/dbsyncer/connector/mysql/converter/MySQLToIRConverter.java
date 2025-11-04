/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.SourceToIRConverter;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.sdk.parser.ddl.ir.DDLOperationType;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL到中间表示转换器
 */
public class MySQLToIRConverter implements SourceToIRConverter {

    // 复用MySQLSchemaResolver的类型转换逻辑
    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();

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
                    // 使用MySQLSchemaResolver将MySQL特定类型转换为标准类型
                    Field mysqlField = new Field();
                    mysqlField.setTypeName(colDataType.getDataType());
                    Field standardField = schemaResolver.toStandardType(mysqlField);
                    column.setTypeName(standardField.getTypeName());
                    column.setType(standardField.getType());

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

    private String removeBackQuotes(String name) {
        if (name != null) {
            return StringUtil.replace(name, "`", "");
        }
        return name;
    }
}