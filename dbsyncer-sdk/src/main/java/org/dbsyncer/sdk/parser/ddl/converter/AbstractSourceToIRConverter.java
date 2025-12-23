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

    /**
     * 推断 UNSPECIFIC 操作的实际类型
     * 当 JSQLParser 无法确定操作类型时（如 SQL Server 的 ALTER TABLE ... ADD column1, column2），
     * 根据表达式的内容推断实际的操作类型
     * 不同数据库对 UNSPECIFIC 操作的解析可能不同，子类需要根据数据库特性实现此方法
     * 
     * @param expr AlterExpression 对象
     * @return 推断后的 AlterOperation，如果无法推断，返回 null
     */
    protected AlterOperation inferUnspecificOperation(AlterExpression expr) {
        // 默认实现：不支持 UNSPECIFIC 操作，子类需要重写
        return null;
    }

    @Override
    public DDLIntermediateRepresentation convert(Alter alter) {
        DDLIntermediateRepresentation ir = new DDLIntermediateRepresentation();
        ir.setTableName(removeIdentifier(alter.getTable().getName()));

        // 跟踪前一个操作类型，用于 UNSPECIFIC 推断失败时复用
        AlterOperation previousOperation = null;

        for (AlterExpression expr : alter.getAlterExpressions()) {
            AlterOperation currentOperation = null;
            List<Field> newColumns = null;
            
            switch (expr.getOperation()) {
                case ADD:
                    currentOperation = AlterOperation.ADD;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList(), expr);
                    break;
                case DROP:
                    currentOperation = AlterOperation.DROP;
                    newColumns = convertColumnDataTypesForDrop(expr.getColumnName());
                    break;
                case MODIFY:
                    currentOperation = AlterOperation.MODIFY;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList(), expr);
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
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList(), expr);
                    break;
                case CHANGE:
                    currentOperation = AlterOperation.CHANGE;
                    newColumns = convertColumnDataTypes(expr.getColDataTypeList(), expr);
                    // 保存旧字段名到新字段名的映射
                    if (expr.getColumnOldName() != null && expr.getColDataTypeList() != null) {
                        String oldColumnName = removeIdentifier(expr.getColumnOldName());
                        for (AlterExpression.ColumnDataType columnDataType : expr.getColDataTypeList()) {
                            String newColumnName = removeIdentifier(columnDataType.getColumnName());
                            ir.getOldToNewColumnNames().put(oldColumnName, newColumnName);
                        }
                    }
                    break;
                case UNSPECIFIC:
                    // UNSPECIFIC 操作的推断由子类根据数据库类型决定
                    // 例如：SQL Server 的 ALTER TABLE ... ADD column1, column2 可能被解析为 UNSPECIFIC
                    AlterOperation inferredType = inferUnspecificOperation(expr);
                    // 如果推断失败，尝试复用前一个操作类型（同一 ALTER TABLE 语句中的多个表达式通常具有相同的操作类型）
                    if (inferredType == null) {
                        if (previousOperation != null) {
                            inferredType = previousOperation;
                        } else {
                            throw new UnsupportedOperationException(
                                String.format("Unsupported UNSPECIFIC operation for this database type. Expression: %s", expr));
                        }
                    }
                    expr.setOperation(inferredType); // IMPORTANT: 修改 UNSPECIFIC 操作的类型
                    currentOperation = inferredType;
                    // 根据推断的操作类型处理列数据
                    switch (inferredType) {
                        case ADD:
                        case MODIFY:
                            // UNSPECIFIC 可能使用 getColumnDataType（单数）而不是 getColDataTypeList（复数）
                            // 需要将单个 ColumnDataType 转换为列表
                            List<AlterExpression.ColumnDataType> columnDataTypes = getColumnDataTypes(expr);
                            if (columnDataTypes != null && !columnDataTypes.isEmpty()) {
                                newColumns = convertColumnDataTypes(columnDataTypes, expr);
                            } else {
                                // 如果 getColumnDataTypes 返回空列表，尝试直接从表达式创建 Field
                                newColumns = convertColumnFromUnspecific(expr, inferredType);
                            }
                            break;
                        case DROP:
                            newColumns = convertColumnDataTypesForDrop(expr.getColumnName());
                            break;
                        case CHANGE:
                            List<AlterExpression.ColumnDataType> changeColumnDataTypes = getColumnDataTypes(expr);
                            newColumns = convertColumnDataTypes(changeColumnDataTypes, expr);
                            // 保存旧字段名到新字段名的映射
                            if (expr.getColumnOldName() != null && changeColumnDataTypes != null) {
                                String oldColumnName = removeIdentifier(expr.getColumnOldName());
                                for (AlterExpression.ColumnDataType columnDataType : changeColumnDataTypes) {
                                    String newColumnName = removeIdentifier(columnDataType.getColumnName());
                                    ir.getOldToNewColumnNames().put(oldColumnName, newColumnName);
                                }
                            }
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                String.format("Unsupported inferred operation type: %s", inferredType));
                    }
                    break;
                default:
                    // 不支持的操作类型，抛出异常而不是静默忽略
                    throw new UnsupportedOperationException(
                        String.format("Unsupported DDL operation type: %s", expr.getOperation()));
            }
            
            // 添加列到对应操作类型（统一使用 columnsByOperation 作为数据源）
            if (newColumns != null && !newColumns.isEmpty()) {
                ir.addColumns(currentOperation, newColumns);
            }
            
            // 更新前一个操作类型，供后续 UNSPECIFIC 表达式复用
            if (currentOperation != null) {
                previousOperation = currentOperation;
            }
        }
        
        // 确保至少有一个操作类型
        if (ir.getColumnsByOperation().isEmpty()) {
            throw new IllegalStateException("No valid DDL operation found in Alter statement");
        }
        
        return ir;
    }

    /**
     * 获取列数据类型列表
     * 处理 UNSPECIFIC 类型表达式可能没有正确解析列信息的情况
     * 
     * @param expr AlterExpression 对象
     * @return 列数据类型列表
     */
    protected List<AlterExpression.ColumnDataType> getColumnDataTypes(AlterExpression expr) {
        // 优先使用列表形式
        if (expr.getColDataTypeList() != null && !expr.getColDataTypeList().isEmpty()) {
            return expr.getColDataTypeList();
        }
        
        // 如果列表为空，但表达式有列名，可能需要手动构建 ColumnDataType
        // 这种情况通常发生在 UNSPECIFIC 类型表达式中
        // 子类可以重写此方法以提供更精确的处理逻辑
        return new ArrayList<>();
    }

    protected List<Field> convertColumnDataTypes(List<AlterExpression.ColumnDataType> columnDataTypes, AlterExpression expr) {
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
                
                // 提取列规范信息（NOT NULL、DEFAULT、COMMENT等）
                extractColumnSpecs(columnDataType, column, expr);
                
                columns.add(column);
            }
        }
        return columns;
    }

    /**
     * 从 ColumnDataType 中提取列规范信息（NOT NULL、DEFAULT、COMMENT等）
     * 
     * @param columnDataType JSQLParser 的 ColumnDataType 对象
     * @param column 目标 Field 对象
     * @param expr AlterExpression 对象（用于获取原始 DDL 信息）
     */
    protected void extractColumnSpecs(AlterExpression.ColumnDataType columnDataType, Field column, AlterExpression expr) {
        if (columnDataType == null || column == null) {
            return;
        }
        
        // 从 ColumnDataType 获取列规范列表
        List<String> columnSpecs = columnDataType.getColumnSpecs();
        
        // 如果 ColumnDataType 没有列规范，尝试从字符串表示中解析（后备方案）
        if (columnSpecs == null || columnSpecs.isEmpty()) {
            // 尝试从 AlterExpression 的 getOptionalSpecifier() 中解析 NULL/NOT NULL
            // 这对于 SQL Server 的 ALTER COLUMN ... NULL 语法特别重要
            // 因为 JSQLParser 可能不会将 NULL 放入 columnSpecs
            if (expr != null) {
                try {
                    String exprStr = expr.getOptionalSpecifier();
                    if (exprStr != null && !exprStr.trim().isEmpty()) {
                        String upperString = exprStr.toUpperCase().trim();
                        // 检查是否包含 NOT NULL
                        if (upperString.contains("NOT NULL")) {
                            column.setNullable(false);
                            return;
                        }
                        // 检查是否包含单独的 NULL（不是 NOT NULL）
                        // 使用单词边界匹配，避免误匹配（如 "NULLABLE"）
                        if (upperString.matches(".*\\bNULL\\b.*") && !upperString.contains("NOT NULL")) {
                            column.setNullable(true);
                            return;
                        }
                    }
                } catch (Exception e) {
                    // 解析失败，忽略
                }
            }
            // 如果 getOptionalSpecifier() 解析失败，尝试从 AlterExpression 的 toString() 解析
            if (expr != null) {
                try {
                    String exprString = expr.toString();
                    if (exprString != null && !exprString.trim().isEmpty()) {
                        String upperString = exprString.toUpperCase().trim();
                        // 检查是否包含 NOT NULL
                        if (upperString.contains("NOT NULL")) {
                            column.setNullable(false);
                            return;
                        }
                        // 检查是否包含单独的 NULL（不是 NOT NULL）
                        if (upperString.matches(".*\\bNULL\\b.*") && !upperString.contains("NOT NULL")) {
                            column.setNullable(true);
                            return;
                        }
                    }
                } catch (Exception e) {
                    // 解析失败，忽略
                }
            }
            // 最后尝试从 ColumnDataType 的 toString() 解析
            try {
                String stringRep = columnDataType.toString();
                if (stringRep != null && !stringRep.trim().isEmpty()) {
                    String upperString = stringRep.toUpperCase().trim();
                    // 检查是否包含 NOT NULL
                    if (upperString.contains("NOT NULL")) {
                        column.setNullable(false);
                        return;
                    }
                    // 检查是否包含单独的 NULL（不是 NOT NULL）
                    if (upperString.matches(".*\\bNULL\\b.*") && !upperString.contains("NOT NULL")) {
                        column.setNullable(true);
                        return;
                    }
                }
            } catch (Exception e) {
                // 解析失败，忽略
            }
            return;
        }
        
        // 解析列规范
        // JSQLParser 将列规范拆分成单独的单词，例如: ["NOT", "NULL", "DEFAULT", "0", "COMMENT", "'年龄'"]
        for (int i = 0; i < columnSpecs.size(); i++) {
            String spec = columnSpecs.get(i);
            if (spec == null) {
                continue;
            }
            
            String upperSpec = spec.toUpperCase().trim();
            
            // 处理 NOT NULL（需要检查连续的 "NOT" 和 "NULL"）
            if (upperSpec.equals("NOT") && i + 1 < columnSpecs.size()) {
                String nextSpec = columnSpecs.get(i + 1);
                if (nextSpec != null && nextSpec.toUpperCase().trim().equals("NULL")) {
                    column.setNullable(false);
                    i++; // 跳过下一个 "NULL"
                    continue;
                }
            } else if (upperSpec.equals("NULL")) {
                // 单独的 NULL（不是 NOT NULL）
                column.setNullable(true);
            }
            // 注意：不再处理 DEFAULT 值，因为数据同步不需要默认值支持
            // 跳过 DEFAULT 关键字及其值
            else if (upperSpec.equals("DEFAULT") && i + 1 < columnSpecs.size()) {
                i++; // 跳过 DEFAULT 后面的值
            }
            // 处理 COMMENT（COMMENT 后面跟着注释内容）
            else if (upperSpec.equals("COMMENT") && i + 1 < columnSpecs.size()) {
                String comment = columnSpecs.get(i + 1);
                if (comment != null && !comment.trim().isEmpty()) {
                    comment = comment.trim();
                    // 去掉可能的引号
                    if ((comment.startsWith("'") && comment.endsWith("'")) 
                        || (comment.startsWith("\"") && comment.endsWith("\""))) {
                        comment = comment.substring(1, comment.length() - 1);
                    }
                    column.setComment(comment);
                    i++; // 跳过下一个注释内容
                    continue;
                }
            }
        }
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

    /**
     * 从 UNSPECIFIC 表达式直接创建 Field 对象
     * 当 getColumnDataTypes 返回空列表时，使用此方法作为后备方案
     * 
     * @param expr AlterExpression 对象
     * @param operation 推断的操作类型
     * @return Field 列表
     */
    protected List<Field> convertColumnFromUnspecific(AlterExpression expr, AlterOperation operation) {
        // 默认实现：返回空列表，子类可以重写此方法
        return new ArrayList<>();
    }
}