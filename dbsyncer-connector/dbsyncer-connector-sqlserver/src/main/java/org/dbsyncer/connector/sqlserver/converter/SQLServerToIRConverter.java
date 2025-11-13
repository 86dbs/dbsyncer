package org.dbsyncer.connector.sqlserver.converter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.AbstractSourceToIRConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL Server到中间表示转换器
 */
public class SQLServerToIRConverter extends AbstractSourceToIRConverter {

    public SQLServerToIRConverter() {
        // 初始化SchemaResolver
        this.schemaResolver = new SqlServerSchemaResolver();
    }

    @Override
    protected String removeIdentifier(String name) {
        if (name != null) {
            return StringUtil.replace(name, "[", "").replace("]", "");
        }
        return name;
    }

    @Override
    protected AlterOperation mapAlterOperation(AlterExpression expr) {
        // SQL Server 使用 ALTER COLUMN 语法，语义上等同于 MODIFY COLUMN
        // 将其映射到 MODIFY 操作类型
        return AlterOperation.MODIFY;
    }

    @Override
    protected AlterOperation inferUnspecificOperation(AlterExpression expr) {
        // SQL Server 的 ALTER TABLE ... ADD column1, column2 可能被 JSQLParser 解析为 UNSPECIFIC
        // 根据表达式内容推断实际的操作类型
        // 注意：如果推断失败，AbstractSourceToIRConverter 会自动复用前一个操作类型
        
        String exprStr = expr.toString();
        if (exprStr == null) {
            return null;
        }
        
        String upperExprStr = exprStr.toUpperCase();
        
        // 按优先级检查操作类型关键字
        // 1. 检查是否包含 "ALTER COLUMN"（应该被解析为 ALTER，但如果被解析为 UNSPECIFIC，需要映射为 MODIFY）
        if (upperExprStr.contains("ALTER COLUMN")) {
            return AlterOperation.MODIFY;
        }
        
        // 2. 检查是否包含 "DROP"
        if (upperExprStr.contains("DROP")) {
            return AlterOperation.DROP;
        }
        
        // 3. 检查是否包含 "ADD"
        if (upperExprStr.contains("ADD")) {
            return AlterOperation.ADD;
        }
        
        // 4. 如果有列数据类型列表，通常是 ADD 操作
        if (expr.getColDataTypeList() != null && !expr.getColDataTypeList().isEmpty()) {
            return AlterOperation.ADD;
        }
        
        // 5. 如果有列名，通常也是 ADD 操作（SQL Server 的多列 ADD 通常被解析为 UNSPECIFIC）
        // 例如：ALTER TABLE ... ADD column1, column2 中的 column2 可能被解析为 UNSPECIFIC
        if (expr.getColumnName() != null) {
            return AlterOperation.ADD;
        }
        
        // 无法推断，返回 null 以复用前一个操作类型
        return null;
    }

    @Override
    protected List<AlterExpression.ColumnDataType> getColumnDataTypes(AlterExpression expr) {
        // 优先使用父类方法（处理列表形式）
        List<AlterExpression.ColumnDataType> result = super.getColumnDataTypes(expr);
        if (result != null && !result.isEmpty()) {
            return result;
        }
        
        // 对于 UNSPECIFIC 类型，JSQLParser 可能没有正确填充 ColDataTypeList
        // 由于 ColumnDataType 是内部类且没有无参构造函数，无法通过反射创建
        // 因此直接返回空列表，让调用方使用 convertColumnFromUnspecific 方法处理
        // convertColumnFromUnspecific 会直接创建 Field 对象，不需要 ColumnDataType
        return new ArrayList<>();
    }

    @Override
    protected List<Field> convertColumnFromUnspecific(AlterExpression expr, AlterOperation operation) {
        // 当 getColumnDataTypes 返回空列表时，直接从 AlterExpression 的属性创建 Field 对象
        // 对于 UNSPECIFIC 类型，JSQLParser 可能已经解析了列名和数据类型，但未填充到 ColDataTypeList 中
        
        String columnName = null;
        ColDataType colDataType = null;
        
        // 1. 尝试从 getColumnName() 获取列名
        if (expr.getColumnName() != null) {
            columnName = removeIdentifier(expr.getColumnName());
        }
        
        // 2. 尝试从 getColDataType() 获取数据类型（单数形式）
        // 注意：AlterExpression 可能没有 getColDataType() 方法，需要检查
        try {
            java.lang.reflect.Method getColDataTypeMethod = expr.getClass().getMethod("getColDataType");
            Object colDataTypeObj = getColDataTypeMethod.invoke(expr);
            if (colDataTypeObj != null && colDataTypeObj instanceof ColDataType) {
                colDataType = (ColDataType) colDataTypeObj;
            }
        } catch (Exception e) {
            // getColDataType() 方法不存在或调用失败，忽略
        }
        
        // 3. 如果列名或数据类型为空，尝试从表达式字符串中解析
        if (columnName == null || colDataType == null) {
            // 使用 getOptionalSpecifier() 获取表达式字符串，例如 "bonus DECIMAL ( 8 , 2 )"
            String exprStr = expr.getOptionalSpecifier();
            if (exprStr != null && !exprStr.trim().isEmpty()) {
                try {
                    String[] parts = exprStr.trim().split("\\s+", 2);
                    if (parts.length >= 2) {
                        // parts[0] 是列名，parts[1] 是数据类型部分
                        if (columnName == null) {
                            columnName = removeIdentifier(parts[0].trim());
                        }
                        
                        if (colDataType == null) {
                            String dataTypePart = parts[1].trim();
                            colDataType = new ColDataType();
                            int parenIndex = dataTypePart.indexOf('(');
                            if (parenIndex > 0) {
                                String dataTypeName = dataTypePart.substring(0, parenIndex).trim();
                                colDataType.setDataType(dataTypeName);
                                String argsPart = dataTypePart.substring(parenIndex + 1);
                                argsPart = argsPart.replace(")", "").trim();
                                String[] args = argsPart.split(",");
                                for (String arg : args) {
                                    colDataType.addArgumentsStringList(arg.trim());
                                }
                            } else {
                                colDataType.setDataType(dataTypePart);
                            }
                        }
                    }
                } catch (Exception e) {
                    // 解析失败
                }
            }
        }
        
        // 4. 如果仍然无法获取列名或数据类型，返回空列表
        if (columnName == null || colDataType == null) {
            return new ArrayList<>();
        }
        
        // 5. 创建 Field 对象
        try {
            Field dbField = new Field();
            dbField.setTypeName(colDataType.getDataType());
            dbField.setName(columnName);
            
            // 使用 SchemaResolver 转换为标准类型
            Field standardField = schemaResolver.toStandardTypeFromDDL(dbField, colDataType);
            
            List<Field> result = new ArrayList<>();
            result.add(standardField);
            return result;
        } catch (Exception e) {
            // 转换失败，返回空列表
            throw new RuntimeException(e);
        }
    }
}