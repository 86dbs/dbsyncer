/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;

import java.util.List;

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
    public String buildQueryCursorSql(SqlBuildContext buildContext) {
        String schemaTable = buildTable(buildContext.getSchema(), buildContext.getTableName());
        String fieldList = buildFieldList(buildContext.getFields());
        String queryFilter = buildContext.getQueryFilter();
        String cursorCondition = buildContext.getCursorCondition();
        List<String> primaryKeys = buildContext.getPrimaryKeys();
        
        String whereClause = "";
        if (StringUtil.isNotBlank(queryFilter) && StringUtil.isNotBlank(cursorCondition)) {
            whereClause = String.format(" %s AND %s", queryFilter, cursorCondition);
        } else if (StringUtil.isNotBlank(queryFilter)) {
            whereClause = " " + queryFilter;
        } else if (StringUtil.isNotBlank(cursorCondition)) {
            whereClause = " WHERE " + cursorCondition;
        }
        
        String orderByClause = buildOrderByClause(primaryKeys);
        return String.format("SELECT %s FROM %s%s%s LIMIT ?", fieldList, schemaTable, whereClause, orderByClause);
    }
}
