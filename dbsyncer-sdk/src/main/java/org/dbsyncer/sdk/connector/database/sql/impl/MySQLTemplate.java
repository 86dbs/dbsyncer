/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

/**
 * MySQL特定SQL模板实现
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class MySQLTemplate extends DefaultSqlTemplate {
    
    @Override
    public String getLeftQuotation() {
        return "`";
    }
    
    @Override
    public String getRightQuotation() {
        return "`";
    }
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} LIMIT ?, ?";
    }
}
