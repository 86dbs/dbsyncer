package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL Server到SQL Server的DDL同步测试
 * 验证SQL Server数据库间的DDL同步功能
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class SQLServerToSQLServerDDLSyncTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;

    @Before
    public void setUp() {
        ddlParser = new DDLParserImpl();
        
        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("sqlserver-test-tablegroup-id");
        testTableGroup.setMappingId("sqlserver-test-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理
        
        // 创建源表和目标表（SQL Server）
        Table sourceTable = new Table();
        sourceTable.setName("employee");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        Table targetTable = new Table();
        targetTable.setName("employee");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);
        
        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();
        
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping idMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(idMapping);
        
        Field firstNameSourceField = new Field("first_name", "NVARCHAR", 12);
        Field firstNameTargetField = new Field("first_name", "NVARCHAR", 12);
        FieldMapping firstNameMapping = new FieldMapping(firstNameSourceField, firstNameTargetField);
        fieldMappings.add(firstNameMapping);
        
        Field lastNameSourceField = new Field("last_name", "NVARCHAR", 12);
        Field lastNameTargetField = new Field("last_name", "NVARCHAR", 12);
        FieldMapping lastNameMapping = new FieldMapping(lastNameSourceField, lastNameTargetField);
        fieldMappings.add(lastNameMapping);
        
        Field departmentSourceField = new Field("department", "NVARCHAR", 12);
        Field departmentTargetField = new Field("department", "NVARCHAR", 12);
        FieldMapping departmentMapping = new FieldMapping(departmentSourceField, departmentTargetField);
        fieldMappings.add(departmentMapping);
        
        testTableGroup.setFieldMapping(fieldMappings);
        
        logger.info("SQL Server到SQL Server的DDL同步测试环境初始化完成");
    }

    /**
     * 测试SQL Server新增字段同步
     */
    @Test
    public void testSQLServerAddColumnSync() {
        logger.info("开始测试SQL Server新增字段同步");
        
        // SQL Server ADD COLUMN 语句
        String sqlServerDDL = "ALTER TABLE employee ADD salary DECIMAL(10,2)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sqlServerDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_ADD", DDLOperationEnum.ALTER_ADD, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sqlServerDDL, ddlConfig.getSql());
            Assert.assertTrue("新增字段列表应包含salary字段", ddlConfig.getAddedFieldNames().contains("salary"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundSalaryMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "salary".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "salary".equals(mapping.getTarget().getName())) {
                    foundSalaryMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到salary字段的映射", foundSalaryMapping);
            
            logger.info("SQL Server新增字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server新增字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试SQL Server删除字段同步
     */
    @Test
    public void testSQLServerDropColumnSync() {
        logger.info("开始测试SQL Server删除字段同步");
        
        // SQL Server DROP COLUMN 语句
        String sqlServerDDL = "ALTER TABLE employee DROP COLUMN department";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sqlServerDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_DROP", DDLOperationEnum.ALTER_DROP, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sqlServerDDL, ddlConfig.getSql());
            Assert.assertTrue("删除字段列表应包含department字段", ddlConfig.getDroppedFieldNames().contains("department"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundDepartmentMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "department".equals(mapping.getSource().getName())) {
                    foundDepartmentMapping = true;
                    break;
                }
            }
            
            Assert.assertFalse("不应找到department字段的映射", foundDepartmentMapping);
            
            logger.info("SQL Server删除字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server删除字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试SQL Server修改字段同步
     */
    @Test
    public void testSQLServerAlterColumnSync() {
        logger.info("开始测试SQL Server修改字段同步");
        
        // SQL Server ALTER COLUMN 语句（注意SQL Server使用ALTER COLUMN而不是MODIFY COLUMN）
        String sqlServerDDL = "ALTER TABLE employee ALTER COLUMN first_name NVARCHAR(100)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sqlServerDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_MODIFY", DDLOperationEnum.ALTER_MODIFY, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sqlServerDDL, ddlConfig.getSql());
            Assert.assertTrue("修改字段列表应包含first_name字段", ddlConfig.getModifiedFieldNames().contains("first_name"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射仍然存在
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundFirstNameMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "first_name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "first_name".equals(mapping.getTarget().getName())) {
                    foundFirstNameMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到first_name字段的映射", foundFirstNameMapping);
            
            logger.info("SQL Server修改字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server修改字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试SQL Server重命名字段同步
     */
    @Test
    public void testSQLServerRenameColumnSync() {
        logger.info("开始测试SQL Server重命名字段同步");
        
        // SQL Server CHANGE COLUMN 语句（注意SQL Server重命名字段需要使用标准的CHANGE语法进行测试）
        String sqlServerDDL = "ALTER TABLE employee CHANGE COLUMN last_name surname NVARCHAR(50)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sqlServerDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_CHANGE", DDLOperationEnum.ALTER_CHANGE, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sqlServerDDL, ddlConfig.getSql());
            Assert.assertTrue("变更字段映射应包含last_name到surname的映射", 
                ddlConfig.getChangedFieldNames().containsKey("last_name") && 
                "surname".equals(ddlConfig.getChangedFieldNames().get("last_name")));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundOldMapping = false;
            boolean foundNewMapping = false;
            
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "surname".equals(mapping.getTarget().getName())) {
                    foundNewMapping = true;
                }
                if (mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "last_name".equals(mapping.getTarget().getName())) {
                    foundOldMapping = true;
                }
            }
            
            Assert.assertTrue("应找到last_name到surname的字段映射", foundNewMapping);
            Assert.assertFalse("不应找到last_name到last_name的旧字段映射", foundOldMapping);
            
            logger.info("SQL Server重命名字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server重命名字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }
}