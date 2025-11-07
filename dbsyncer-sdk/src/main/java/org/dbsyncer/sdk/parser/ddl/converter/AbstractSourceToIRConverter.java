package org.dbsyncer.sdk.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;
import org.dbsyncer.sdk.parser.ddl.ir.DDLOperationType;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.ArrayList;
import java.util.List;

/**
 * 源数据库到中间表示转换器抽象类
 * 提供通用的转换逻辑和SchemaResolver属性
 */
public abstract class AbstractSourceToIRConverter implements SourceToIRConverter {

    // 具体的SchemaResolver实现，由子类在构造函数中初始化
    protected SchemaResolver schemaResolver;

    // 子类需要提供具体的移除标识符方法
    protected abstract String removeIdentifier(String name);

    @Override
    public DDLIntermediateRepresentation convert(Alter alter) {
        DDLIntermediateRepresentation ir = new DDLIntermediateRepresentation();
        ir.setTableName(removeIdentifier(alter.getTable().getName()));

        for (AlterExpression expr : alter.getAlterExpressions()) {
            switch (expr.getOperation()) {
                case ADD:
                    ir.setOperationType(DDLOperationType.ADD);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                case DROP:
                    ir.setOperationType(DDLOperationType.DROP);
                    ir.setColumns(convertColumnDataTypesForDrop(expr.getColumnName()));
                    break;
                case MODIFY:
                    ir.setOperationType(DDLOperationType.MODIFY);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                case CHANGE:
                    ir.setOperationType(DDLOperationType.CHANGE);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                default:
                    // 不支持的操作类型
                    break;
            }
        }
        return ir;
    }

    protected List<Field> convertColumnDataTypes(List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<Field> columns = new ArrayList<>();
        if (columnDataTypes != null) {
            for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
                Field column = new Field();
                column.setName(removeIdentifier(columnDataType.getColumnName()));
                ColDataType colDataType = columnDataType.getColDataType();
                if (colDataType != null) {
                    // 使用具体的SchemaResolver将特定数据库类型转换为标准类型
                    Field dbField = new Field();
                    dbField.setTypeName(colDataType.getDataType());
                    dbField.setName(column.getName());

                    // 直接调用SchemaResolver的toStandardTypeFromDDL方法
                    Field standardField = schemaResolver.toStandardTypeFromDDL(dbField, colDataType);
                    column.setTypeName(standardField.getTypeName());
                    column.setType(standardField.getType());
                    column.setColumnSize(standardField.getColumnSize());
                    column.setRatio(standardField.getRatio());
                }
                columns.add(column);
            }
        }
        return columns;
    }

    protected List<Field> convertColumnDataTypesForDrop(String columnName) {
        List<Field> columns = new ArrayList<>();
        if (columnName != null) {
            Field column = new Field();
            column.setName(removeIdentifier(columnName));
            columns.add(column);
        }
        return columns;
    }
}