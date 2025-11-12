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
    private AlterOperation operationType;
    private List<Field> columns = new ArrayList<>();
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

    public AlterOperation getOperationType() {
        return operationType;
    }

    public void setOperationType(AlterOperation operationType) {
        this.operationType = operationType;
    }

    public List<Field> getColumns() {
        return columns;
    }

    public void setColumns(List<Field> columns) {
        this.columns = columns;
    }

    public Map<String, String> getOldToNewColumnNames() {
        return oldToNewColumnNames;
    }

    public void setOldToNewColumnNames(Map<String, String> oldToNewColumnNames) {
        this.oldToNewColumnNames = oldToNewColumnNames;
    }
}