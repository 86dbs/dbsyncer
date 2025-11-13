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
            AlterOperation currentOperation = null;
            List<Field> newColumns = null;
            
            switch (expr.getOperation()) {
                case ADD:
                    currentOperation = AlterOperation.ADD;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList());
                    break;
                case DROP:
                    currentOperation = AlterOperation.DROP;
                    newColumns = convertColumnDataTypesForDrop(expr.getColumnName());
                    break;
                case MODIFY:
                    currentOperation = AlterOperation.MODIFY;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList());
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
                    currentOperation = mappedType;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList());
                    break;
                case CHANGE:
                    currentOperation = AlterOperation.CHANGE;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList());
                    // 保存旧字段名到新字段名的映射
                    if (expr.getColumnOldName() != null && expr.getColDataTypeList() != null) {
                        String oldColumnName = removeIdentifier(expr.getColumnOldName());
                        for (AlterExpression.ColumnDataType columnDataType : expr.getColDataTypeList()) {
                            String newColumnName = removeIdentifier(columnDataType.getColumnName());
                            ir.getOldToNewColumnNames().put(oldColumnName, newColumnName);
                        }
                    }
                    break;
                default:
                    // 不支持的操作类型，抛出异常而不是静默忽略
                    throw new UnsupportedOperationException(
                        String.format("Unsupported DDL operation type: %s", expr.getOperation()));
            }
            
            // 合并相同操作的列列表，而不是覆盖
            if (ir.getOperationType() == null) {
                // 第一次设置操作类型和列列表
                ir.setOperationType(currentOperation);
                ir.setColumns(newColumns != null ? new ArrayList<>(newColumns) : new ArrayList<>());
            } else if (ir.getOperationType() == currentOperation) {
                // 相同操作类型，合并列列表
                if (newColumns != null && !newColumns.isEmpty()) {
                    ir.getColumns().addAll(newColumns);
                }
            } else {
                // 不同的操作类型，抛出异常（一个ALTER语句通常只包含一种操作类型）
                throw new IllegalStateException(
                    String.format("Mixed DDL operations in a single ALTER statement are not supported. " +
                            "Previous operation: %s, Current operation: %s", 
                            ir.getOperationType(), currentOperation));
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
                    // 保留isSizeFixed信息（由DataType的handleDDLParameters方法设置）
                    if (standardField.getIsSizeFixed() != null) {
                        column.setIsSizeFixed(standardField.getIsSizeFixed());
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