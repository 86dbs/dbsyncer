/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql;

import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.connector.database.sql.impl.DefaultSqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.model.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL模板测试
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class SqlTemplateTest {
    
    @Test
    public void testDefaultTemplate() {
        SqlTemplate template = new DefaultSqlTemplate();
        
        // 测试流式查询
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema("public");
        context.setTableName("users");
        context.setFields(Arrays.asList(
            new Field("id", "BIGINT", true),
            new Field("name", "VARCHAR", false)
        ));
        
        String sql = template.buildSql(SqlTemplateType.QUERY_STREAM, context);
        assertEquals("SELECT \"id\", \"name\" FROM public.users", sql);
    }
    
    @Test
    public void testMySQLTemplate() {
        SqlTemplate template = new MySQLTemplate();
        
        // 测试流式查询
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema("test");
        context.setTableName("users");
        context.setFields(Arrays.asList(
            new Field("id", "BIGINT", true),
            new Field("name", "VARCHAR", false)
        ));
        
        String sql = template.buildSql(SqlTemplateType.QUERY_STREAM, context);
        assertEquals("SELECT `id`, `name` FROM test.users", sql);
    }
    
    @Test
    public void testQueryWithFilter() {
        SqlTemplate template = new DefaultSqlTemplate();
        
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema("public");
        context.setTableName("users");
        context.setFields(Arrays.asList(
            new Field("id", "BIGINT", true),
            new Field("name", "VARCHAR", false)
        ));
        context.setQueryFilter("WHERE status = ?");
        
        String sql = template.buildSql(SqlTemplateType.QUERY_STREAM, context);
        assertEquals("SELECT \"id\", \"name\" FROM public.users WHERE status = ?", sql);
    }
    
    @Test
    public void testCountQuery() {
        SqlTemplate template = new DefaultSqlTemplate();
        
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema("public");
        context.setTableName("users");
        context.setQueryFilter("WHERE status = ?");
        
        String sql = template.buildSql(SqlTemplateType.QUERY_COUNT, context);
        assertEquals("SELECT COUNT(1) FROM public.users WHERE status = ?", sql);
    }
}
