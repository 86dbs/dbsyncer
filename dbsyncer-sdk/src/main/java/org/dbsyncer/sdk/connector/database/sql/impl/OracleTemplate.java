/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

/**
 * Oracle特定SQL模板实现
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class OracleTemplate extends DefaultSqlTemplate {
    
    @Override
    public String getCursorTemplate() {
        return "SELECT * FROM (SELECT ROWNUM rn, t.* FROM ({baseQuery} {where} ORDER BY {orderBy}) t WHERE ROWNUM <= ?) WHERE rn > ?";
    }
}
