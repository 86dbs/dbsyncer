package org.dbsyncer.sdk.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;
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

    /**
     * 将 ALTER 操作映射到具体的 DDL 操作类型
     * 不同数据库对 ALTER 操作的语义不同，子类需要根据数据库特性实现此方法
     * 
     * @param expr AlterExpression 对象
     * @return 映射后的 AlterOperation，如果该数据库不支持此 ALTER 操作，返回 null
     */
    protected AlterOperation mapAlterOperation(AlterExpression expr) {
        // 默认实现：不支持 ALTER 操作，子类需要重写
        return null;
    }

    @Override
    public DDLIntermediateRepresentation convert(Alter alter) {
        DDLIntermediateRepresentation ir = new DDLIntermediateRepresentation();
        ir.setTableName(removeIdentifier(alter.getTable().getName()));

        for (AlterExpression expr : alter.getAlterExpressions()) {
            switch (expr.getOperation()) {
                case ADD:
                    ir.setOperationType(AlterOperation.ADD);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                case DROP:
                    ir.setOperationType(AlterOperation.DROP);
                    ir.setColumns(convertColumnDataTypesForDrop(expr.getColumnName()));
                    break;
                case MODIFY:
                    ir.setOperationType(AlterOperation.MODIFY);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                case ALTER:
                    // ALTER 操作的映射由子类根据数据库类型决定
                    // 例如：SQL Server 的 ALTER COLUMN 映射到 MODIFY，其他数据库可能不同
                    AlterOperation mappedType = mapAlterOperation(expr);
                    if (mappedType == null) {
                        throw new UnsupportedOperationException(
                            String.format("Unsupported ALTER operation for this database type. Expression: %s", expr));
                    }
                    expr.setOperation(mappedType); // IMPORTANT: 修改 ALTER 操作的类型
                    ir.setOperationType(mappedType);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                case CHANGE:
                    ir.setOperationType(AlterOperation.CHANGE);
                    ir.setColumns(convertColumnDataTypes(expr.getColDataTypeList()));
                    break;
                default:
                    // 不支持的操作类型，抛出异常而不是静默忽略
                    throw new UnsupportedOperationException(
                        String.format("Unsupported DDL operation type: %s", expr.getOperation()));
            }
        }
        
        // 确保operationType已设置
        if (ir.getOperationType() == null) {
            throw new IllegalStateException("No valid DDL operation found in Alter statement");
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
                    // 保留SRID信息（用于Geometry类型）
                    if (standardField.getSrid() != null) {
                        column.setSrid(standardField.getSrid());
                    }
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