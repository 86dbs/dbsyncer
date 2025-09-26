/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

/**
 * PostgreSQL特定SQL模板实现
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class PostgreSQLTemplate extends DefaultSqlTemplate {
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} LIMIT ? OFFSET ?";
    }
}
