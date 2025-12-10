package org.dbsyncer.connector.sqlserver.ct.model;

/**
 * DDL 变更类型枚举
 */
public enum DDLChangeType {
    /**
     * 新增列
     */
    ADD_COLUMN,
    
    /**
     * 删除列
     */
    DROP_COLUMN,
    
    /**
     * 修改列（类型、长度、精度、可空性）
     */
    ALTER_COLUMN,
    
    /**
     * 修改主键
     */
    ALTER_PRIMARY_KEY,
    
    /**
     * 重命名列
     */
    RENAME_COLUMN
}

