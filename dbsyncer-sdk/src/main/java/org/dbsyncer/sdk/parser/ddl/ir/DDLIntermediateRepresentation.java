package org.dbsyncer.sdk.parser.ddl.ir;

import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DDL中间表示
 */
public class DDLIntermediateRepresentation {

    private String tableName;
    
    /**
     * 数据库架构名（Schema）
     * 例如：SQL Server 的 'dbo'，PostgreSQL 的 'public'，Oracle 的用户名等
     * 如果未设置，某些数据库转换器可能使用默认值
     */
    private String schema;
    
    /**
     * 按操作类型分组的列列表（统一数据源）
     * Key: 操作类型, Value: 该操作类型对应的列列表
     */
    private Map<AlterOperation, List<Field>> columnsByOperation = new LinkedHashMap<>();
    /**
     * 旧字段名到新字段名的映射（用于CHANGE操作）
     * Key: 旧字段名, Value: 新字段名
     */
    private Map<String, String> oldToNewColumnNames = new LinkedHashMap<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * 获取主操作类型（向后兼容方法）
     * 如果只有一个操作类型，返回该类型；如果有多个，返回第一个
     * @return 主操作类型，如果没有操作，返回null
     */
    public AlterOperation getOperationType() {
        if (columnsByOperation.isEmpty()) {
            return null;
        }
        return columnsByOperation.keySet().iterator().next();
    }

    /**
     * 设置主操作类型（向后兼容方法）
     * 实际上是将列添加到指定操作类型
     * @param operationType 操作类型
     */
    public void setOperationType(AlterOperation operationType) {
        // 如果指定操作类型还没有列列表，创建一个空列表
        columnsByOperation.computeIfAbsent(operationType, k -> new ArrayList<>());
    }

    /**
     * 获取主操作类型的列列表（向后兼容方法）
     * 如果只有一个操作类型，返回该类型的列列表；如果有多个，返回第一个操作类型的列列表
     * @return 主操作类型的列列表
     */
    public List<Field> getColumns() {
        AlterOperation operationType = getOperationType();
        if (operationType == null) {
            return new ArrayList<>();
        }
        return columnsByOperation.getOrDefault(operationType, new ArrayList<>());
    }

    /**
     * 设置主操作类型的列列表（向后兼容方法）
     * 如果还没有操作类型，此方法不会生效（需要先调用 setOperationType）
     * @param columns 列列表
     */
    public void setColumns(List<Field> columns) {
        AlterOperation operationType = getOperationType();
        if (operationType != null) {
            columnsByOperation.put(operationType, columns != null ? new ArrayList<>(columns) : new ArrayList<>());
        }
        // 如果没有操作类型，忽略设置（向后兼容：某些代码可能先设置 columns 再设置 operationType）
    }

    public Map<String, String> getOldToNewColumnNames() {
        return oldToNewColumnNames;
    }

    public void setOldToNewColumnNames(Map<String, String> oldToNewColumnNames) {
        this.oldToNewColumnNames = oldToNewColumnNames;
    }

    /**
     * 获取按操作类型分组的列列表
     * @return 按操作类型分组的列列表
     */
    public Map<AlterOperation, List<Field>> getColumnsByOperation() {
        return columnsByOperation;
    }

    /**
     * 设置按操作类型分组的列列表
     * @param columnsByOperation 按操作类型分组的列列表
     */
    public void setColumnsByOperation(Map<AlterOperation, List<Field>> columnsByOperation) {
        this.columnsByOperation = columnsByOperation;
    }

    /**
     * 添加列到指定操作类型
     * @param operation 操作类型
     * @param columns 列列表
     */
    public void addColumns(AlterOperation operation, List<Field> columns) {
        columnsByOperation.computeIfAbsent(operation, k -> new ArrayList<>()).addAll(columns);
    }

    /**
     * 检查是否包含混合操作
     * @return 如果包含多个操作类型，返回true
     */
    public boolean hasMixedOperations() {
        return columnsByOperation.size() > 1;
    }
}