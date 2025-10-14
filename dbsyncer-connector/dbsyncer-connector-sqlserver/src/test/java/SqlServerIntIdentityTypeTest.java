import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-04-05
 */
public class SqlServerIntIdentityTypeTest {

    @Test
    public void testIntIdentityTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 int identity 类型的合并
        Field intIdentityField = new Field("ID", "int identity", 4);
        Integer value = 501;
        
        Object mergedValue = schemaResolver.merge(value, intIdentityField);
        
        // 验证值是否正确转换为标准int类型
        Assert.assertTrue(mergedValue instanceof Integer);
        Assert.assertEquals(value, mergedValue);
        
        // 测试 int identity 类型的转换
        Object convertedValue = schemaResolver.convert(mergedValue, intIdentityField);
        
        // 验证值是否正确转换
        Assert.assertTrue(convertedValue instanceof Integer);
        Assert.assertEquals(value, convertedValue);
    }
    
    @Test
    public void testIntIdentityToMySQLCompatibility() {
        // 模拟从SQL Server到MySQL的同步过程
        SchemaResolver sqlServerSchemaResolver = new SqlServerSchemaResolver();
        // 这里我们模拟一个假的MySQL SchemaResolver，只支持标准int类型
        SchemaResolver mySQLSchemaResolver = new SchemaResolver() {
            @Override
            public Object merge(Object val, Field field) {
                // MySQL不需要特殊合并处理
                return val;
            }

            @Override
            public Object convert(Object val, Field field) {
                // MySQL只支持标准int类型，不支持int identity
                if ("int identity".equals(field.getTypeName())) {
                    throw new RuntimeException("MySQL does not support type [class java.lang.Integer] convert to [int identity]");
                }
                return val;
            }
        };
        
        // 源数据
        Field sourceField = new Field("ID", "int identity", 4);
        Integer value = 501;
        
        // 第一步：SQL Server端类型标准化
        Object standardizedValue = sqlServerSchemaResolver.merge(value, sourceField);
        Assert.assertTrue(standardizedValue instanceof Integer);
        Assert.assertEquals(value, standardizedValue);
        
        // 第二步：模拟传输到MySQL端
        // 注意：在实际流程中，经过Picker处理后，字段类型应该已经标准化
        // 但在测试中，我们仍然使用原始字段来模拟问题场景
        try {
            Object result = mySQLSchemaResolver.convert(standardizedValue, sourceField);
            Assert.fail("Should throw exception for int identity type");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("MySQL does not support type"));
        }
        
        // 正确的做法应该是：经过Picker处理后，字段类型被标准化
        Field standardizedField = new Field("ID", "int", 4); // 标准化后的字段
        Object result = mySQLSchemaResolver.convert(standardizedValue, standardizedField);
        Assert.assertEquals(value, result);
    }
    
    @Test
    public void testSmallIntTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 smallint 类型的合并
        Field smallIntField = new Field("num", "smallint", 4);
        Integer value = 1000;
        
        Object mergedValue = schemaResolver.merge(value, smallIntField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof Short);
        Assert.assertEquals(Short.valueOf(value.toString()), mergedValue);
    }
    
    @Test
    public void testBigIntTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 bigint 类型的合并
        Field bigIntField = new Field("bigNum", "bigint", 4);
        Long value = 1000000L;
        
        Object mergedValue = schemaResolver.merge(value, bigIntField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof Long);
        Assert.assertEquals(value, mergedValue);
    }
    
    @Test
    public void testBitTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 bit 类型的合并
        Field bitField = new Field("isActive", "bit", 4);
        Boolean value = true;
        
        Object mergedValue = schemaResolver.merge(value, bitField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof Boolean);
        Assert.assertEquals(value, mergedValue);
    }
    
    @Test
    public void testVarcharTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 varchar 类型的合并
        Field varcharField = new Field("name", "varchar", 4);
        String value = "test";
        
        Object mergedValue = schemaResolver.merge(value, varcharField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof String);
        Assert.assertEquals(value, mergedValue);
    }
    
    @Test
    public void testTinyintTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 tinyint 类型的合并
        Field tinyintField = new Field("age", "tinyint", 4);
        Integer value = 25;
        
        Object mergedValue = schemaResolver.merge(value, tinyintField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof Byte);
        Assert.assertEquals(Byte.valueOf(value.toString()), mergedValue);
    }
    
    @Test
    public void testDecimalTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 decimal 类型的合并
        Field decimalField = new Field("price", "decimal", 4);
        BigDecimal value = new BigDecimal("123.45");
        
        Object mergedValue = schemaResolver.merge(value, decimalField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof BigDecimal);
        Assert.assertEquals(value, mergedValue);
    }
    
    @Test
    public void testDateTimeTypeConversion() {
        SchemaResolver schemaResolver = new SqlServerSchemaResolver();
        
        // 测试 datetime 类型的合并
        Field datetimeField = new Field("created", "datetime", 4);
        Timestamp value = new Timestamp(System.currentTimeMillis());
        
        Object mergedValue = schemaResolver.merge(value, datetimeField);
        
        // 验证值是否正确转换
        Assert.assertTrue(mergedValue instanceof Timestamp);
        Assert.assertEquals(value, mergedValue);
    }
}