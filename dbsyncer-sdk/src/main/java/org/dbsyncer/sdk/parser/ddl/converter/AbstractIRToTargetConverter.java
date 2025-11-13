package org.dbsyncer.sdk.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;

import java.util.List;
import java.util.Map;

/**
 * IR到目标数据库转换器抽象基类
 * 提供通用的convert方法实现，子类只需实现convertOperation方法
 */
public abstract class AbstractIRToTargetConverter implements IRToTargetConverter {

    @Override
    public String convert(DDLIntermediateRepresentation ir) {
        if (ir == null) {
            throw new IllegalArgumentException("DDLIntermediateRepresentation cannot be null");
        }
        
        Map<AlterOperation, List<Field>> columnsByOperation = ir.getColumnsByOperation();
        if (columnsByOperation.isEmpty()) {
            throw new IllegalArgumentException("No operations found in DDLIntermediateRepresentation");
        }
        
        // 统一处理：为每个操作类型生成SQL，用分号连接
        StringBuilder sql = new StringBuilder();
        boolean first = true;
        for (Map.Entry<AlterOperation, List<Field>> entry : columnsByOperation.entrySet()) {
            if (!first) {
                sql.append("; ");
            }
            first = false;
            AlterOperation operation = entry.getKey();
            List<Field> columns = entry.getValue();
            sql.append(convertOperation(ir.getTableName(), operation, columns, ir.getOldToNewColumnNames()));
        }
        return sql.toString();
    }

    /**
     * 转换单个操作类型为SQL
     * 子类需要实现此方法以提供数据库特定的SQL生成逻辑
     * 
     * @param tableName 表名
     * @param operation 操作类型
     * @param columns 列列表
     * @param oldToNewColumnNames 旧字段名到新字段名的映射（用于CHANGE操作，不需要的数据库可以忽略）
     * @return SQL语句
     */
    protected abstract String convertOperation(String tableName, AlterOperation operation, 
                                               List<Field> columns, Map<String, String> oldToNewColumnNames);
}

