package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.List;

/**
 * MySQL特定SQL模板实现
 */
public class MySQLTemplate extends AbstractSqlTemplate {

    public MySQLTemplate(SchemaResolver schemaResolver) {
        super(schemaResolver);
    }

    @Override
    public String getLeftQuotation() {
        return "`";
    }

    @Override
    public String getRightQuotation() {
        return "`";
    }

    @Override
    public String buildUpsertSql(String schemaTable, List<Field> fields, List<String> primaryKeys) {
        String fieldNames = fields.stream()
                .map(field -> buildColumn(field.getName()))
                .collect(java.util.stream.Collectors.joining(", "));
        String placeholders = fields.stream()
                .map(field -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
        String updateClause = fields.stream()
                .filter(field -> !field.isPk())
                .map(field -> buildColumn(field.getName()) + " = VALUES(" + buildColumn(field.getName()) + ")")
                .collect(java.util.stream.Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                schemaTable, fieldNames, placeholders, updateClause);
    }

    @Override
    public String buildAddColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" ADD COLUMN ").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 添加 DEFAULT 值
        if (field.getDefaultValue() != null && !field.getDefaultValue().isEmpty()) {
            sql.append(" DEFAULT ").append(field.getDefaultValue());
        }
        
        // 添加 COMMENT
        if (field.getComment() != null && !field.getComment().isEmpty()) {
            sql.append(" COMMENT '").append(field.getComment().replace("'", "''")).append("'");
        }
        
        return sql.toString();
    }

    @Override
    public String buildModifyColumnSql(String tableName, Field field) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" MODIFY COLUMN ").append(buildColumn(field.getName()))
           .append(" ").append(convertToDatabaseType(field));
        
        // 添加 NOT NULL 约束
        if (field.getNullable() != null && !field.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 添加 DEFAULT 值
        if (field.getDefaultValue() != null && !field.getDefaultValue().isEmpty()) {
            sql.append(" DEFAULT ").append(field.getDefaultValue());
        }
        
        // 添加 COMMENT
        if (field.getComment() != null && !field.getComment().isEmpty()) {
            sql.append(" COMMENT '").append(field.getComment().replace("'", "''")).append("'");
        }
        
        return sql.toString();
    }

    @Override
    public String buildRenameColumnSql(String tableName, String oldFieldName, Field newField) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(buildQuotedTableName(tableName))
           .append(" CHANGE COLUMN ").append(buildColumn(oldFieldName))
           .append(" ").append(buildColumn(newField.getName()))
           .append(" ").append(convertToDatabaseType(newField));
        
        // 添加 NOT NULL 约束
        if (newField.getNullable() != null && !newField.getNullable()) {
            sql.append(" NOT NULL");
        }
        
        // 添加 DEFAULT 值
        if (newField.getDefaultValue() != null && !newField.getDefaultValue().isEmpty()) {
            sql.append(" DEFAULT ").append(newField.getDefaultValue());
        }
        
        // 添加 COMMENT
        if (newField.getComment() != null && !newField.getComment().isEmpty()) {
            sql.append(" COMMENT '").append(newField.getComment().replace("'", "''")).append("'");
        }
        
        return sql.toString();
    }

    @Override
    public String buildDropColumnSql(String tableName, String fieldName) {
        return String.format("ALTER TABLE %s DROP COLUMN %s",
                buildQuotedTableName(tableName),
                buildColumn(fieldName));
    }

    @Override
    public String convertToDatabaseType(Field column) {
        // 使用SchemaResolver进行类型转换，完全委托给SchemaResolver处理
        Field targetField = schemaResolver.fromStandardType(column);
        String typeName = targetField.getTypeName();
        
        // 处理参数（长度、精度等）
        switch (typeName) {
            case "VARCHAR":
                if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                throw new IllegalArgumentException("should give size for column: " + column.getTypeName());
            case "TEXT":
                // 根据columnSize判断使用哪种TEXT类型
                // MySQL的TEXT类型容量：
                // - TINYTEXT: 最大255字节
                // - TEXT: 最大65,535字节 (64KB)
                // - MEDIUMTEXT: 最大16,777,215字节 (16MB)
                // - LONGTEXT: 最大4,294,967,295字节 (4GB)
                long columnSize = column.getColumnSize();
                if (columnSize > 0) {
                    if (columnSize <= 255L) {
                        return "TINYTEXT";
                    } else if (columnSize <= 65535L) {
                        return "TEXT";
                    } else if (columnSize <= 16777215L) {
                        return "MEDIUMTEXT";
                    } else {
                        return "LONGTEXT";
                    }
                }
                // 如果没有columnSize信息，默认使用TEXT
                return "TEXT";
            case "DECIMAL":
                if (column.getColumnSize() > 0 && column.getRatio() >= 0) {
                    return typeName + "(" + column.getColumnSize() + "," + column.getRatio() + ")";
                } else if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                return "DECIMAL(10,0)";
            case "VARBINARY":
                // BYTES类型：用于小容量二进制数据
                // 对于固定长度的二进制数据，使用BINARY类型
                // MySQL的BINARY类型用于固定长度二进制数据，VARBINARY用于可变长度
                // 当columnSize存在且<=255时，使用BINARY以获得更好的性能
                long binarySize = column.getColumnSize();
                if (binarySize > 0 && binarySize <= 255) {
                    // 固定长度且小于等于255，使用BINARY
                    return "BINARY(" + binarySize + ")";
                } else if (binarySize > 255 && binarySize <= 65535) {
                    // 有长度但大于255且<=65535，使用VARBINARY
                    return "VARBINARY(" + binarySize + ")";
                }
                // 没有长度信息或长度超过65535，默认使用VARBINARY(65535)
                return "VARBINARY(65535)";
            case "BLOB":
                // BLOB类型：用于大容量二进制数据
                // MySQL的BLOB类型容量：
                // - TINYBLOB: 最大255字节
                // - BLOB: 最大65,535字节 (64KB)
                // - MEDIUMBLOB: 最大16,777,215字节 (16MB)
                // - LONGBLOB: 最大4,294,967,295字节 (4GB)
                long blobSize = column.getColumnSize();
                if (blobSize > 0) {
                    if (blobSize <= 255L) {
                        return "TINYBLOB";
                    } else if (blobSize <= 65535L) {
                        return "BLOB";
                    } else if (blobSize <= 16777215L) {
                        return "MEDIUMBLOB";
                    } else {
                        return "LONGBLOB";
                    }
                }
                // 没有长度信息，默认使用BLOB（适合大多数场景）
                return "BLOB";
            case "BINARY":
                // 如果已经是BINARY类型，保持BINARY并添加长度
                if (column.getColumnSize() > 0) {
                    return "BINARY(" + column.getColumnSize() + ")";
                }
                return "BINARY";
            case "TINYINT":
                // BOOLEAN类型映射到TINYINT，对于布尔类型使用TINYINT(1)
                // 检查是否是BOOLEAN标准类型（通过column的typeName判断）
                String standardType = column.getTypeName();
                if ("BOOLEAN".equals(standardType)) {
                    return "TINYINT(1)";
                }
                // 如果是普通的TINYINT类型，根据columnSize处理
                if (column.getColumnSize() > 0) {
                    return typeName + "(" + column.getColumnSize() + ")";
                }
                return typeName;
            default:
                return typeName;
        }
    }
}