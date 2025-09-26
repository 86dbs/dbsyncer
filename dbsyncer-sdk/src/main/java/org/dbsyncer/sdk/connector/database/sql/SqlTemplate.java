/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql;

import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;

/**
 * SQL模板接口
 * 提供统一的SQL模板定义和构建方法
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public interface SqlTemplate {
    
    /**
     * 获取数据库特定的引号字符
     */
    String getLeftQuotation();
    String getRightQuotation();
    
    /**
     * 获取数据库特定的游标SQL模板
     */
    String getCursorTemplate();
    
    /**
     * 获取数据库特定的流式查询SQL模板
     */
    String getStreamingTemplate();
    
    /**
     * 基础查询SQL模板
     */
    String getQueryTemplate();
    
    /**
     * 计数查询SQL模板
     */
    String getCountTemplate();
    
    /**
     * 插入SQL模板
     */
    String getInsertTemplate();
    
    /**
     * 更新SQL模板
     */
    String getUpdateTemplate();
    
    /**
     * 删除SQL模板
     */
    String getDeleteTemplate();
    
    /**
     * 存在性检查SQL模板
     */
    String getExistTemplate();
    
    /**
     * 构建SQL（仅构建SQL结构，包含参数占位符）
     * @param templateType 模板类型
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    String buildSql(SqlTemplateType templateType, SqlBuildContext buildContext);
}