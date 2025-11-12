package org.dbsyncer.sdk.parser.ddl.ir;

import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * DDL中间表示
 */
public class DDLIntermediateRepresentation {

    private String tableName;
    private AlterOperation operationType;
    private List<Field> columns = new ArrayList<>();

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
}