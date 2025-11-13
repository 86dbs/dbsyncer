package org.dbsyncer.connector.mysql.converter;

import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.AbstractIRToTargetConverter;

import java.util.List;
import java.util.Map;

/**
 * 中间表示到MySQL转换器，用于 DDL 处理
 */
public class IRToMySQLConverter extends AbstractIRToTargetConverter {

    private final SqlTemplate mysqlTemplate;

    public IRToMySQLConverter(SqlTemplate mysqlTemplate) {
        // 构造函数，可以传入带有SchemaResolver的MySQLTemplate
        this.mysqlTemplate = mysqlTemplate;
    }

    @Override
    protected String convertOperation(String tableName, AlterOperation operation, List<Field> columns, 
                                      Map<String, String> oldToNewColumnNames) {
        // MySQL 的 CHANGE 操作不需要 oldToNewColumnNames，忽略该参数
        switch (operation) {
            case ADD:
                return convertColumnsToAdd(tableName, columns);
            case MODIFY:
                return convertColumnsToModify(tableName, columns);
            case CHANGE:
                return convertColumnsToChange(tableName, columns);
            case DROP:
                return convertColumnsToDrop(tableName, columns);
            default:
                throw new IllegalArgumentException("Unsupported AlterOperation: " + operation);
        }
    }

    private String convertColumnsToAdd(String tableName, List<Field> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        
        // MySQL支持在单个ALTER TABLE语句中添加多个列
        // 格式: ALTER TABLE `table` ADD COLUMN `col1` type1, ADD COLUMN `col2` type2
        StringBuilder result = new StringBuilder();
        String quotedTableName = mysqlTemplate.buildQuotedTableName(tableName);
        result.append("ALTER TABLE ").append(quotedTableName).append(" ADD COLUMN ");
        
        // 从buildAddColumnSql的结果中提取列定义部分
        // buildAddColumnSql返回: "ALTER TABLE `table` ADD COLUMN `col` type"
        // 我们需要提取 "`col` type" 部分
        String prefix = "ALTER TABLE " + quotedTableName + " ADD COLUMN ";
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ADD COLUMN ");
            }
            Field column = columns.get(i);
            String fullSql = mysqlTemplate.buildAddColumnSql(tableName, column);
            // 提取列定义部分（去掉 "ALTER TABLE `table` ADD COLUMN " 前缀）
            String columnDef = fullSql.substring(prefix.length());
            result.append(columnDef);
        }
        return result.toString();
    }

    private String convertColumnsToModify(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(mysqlTemplate.buildModifyColumnSql(tableName, column));
        }
        return result.toString();
    }

    private String convertColumnsToChange(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            // 对于CHANGE操作，我们假设字段名不变，仅类型改变
            result.append(mysqlTemplate.buildRenameColumnSql(tableName, column.getName(), column));
        }
        return result.toString();
    }

    private String convertColumnsToDrop(String tableName, List<Field> columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            Field column = columns.get(i);
            result.append(mysqlTemplate.buildDropColumnSql(tableName, column.getName()));
        }
        return result.toString();
    }
}