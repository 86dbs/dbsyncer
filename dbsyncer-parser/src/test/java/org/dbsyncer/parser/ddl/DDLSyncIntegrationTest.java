package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * DDL同步集成测试
 * 测试DDL同步的端到端功能，包括解析、转换和执行
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class DDLSyncIntegrationTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;

    @Before
    public void setUp() {
        ddlParser = new DDLParserImpl();
        
        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("test-tablegroup-id");
        testTableGroup.setMappingId("test-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理
        
        // 创建源表和目标表
        Table sourceTable = new Table();
        sourceTable.setName("test_table");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        Table targetTable = new Table();
        targetTable.setName("test_table");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);
        
        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();
        
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping existingMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(existingMapping);
        
        Field nameSourceField = new Field("name", "VARCHAR", 12);
        Field nameTargetField = new Field("name", "VARCHAR", 12);
        FieldMapping nameMapping = new FieldMapping(nameSourceField, nameTargetField);
        fieldMappings.add(nameMapping);
        
        testTableGroup.setFieldMapping(fieldMappings);
        
        logger.info("DDL同步集成测试环境初始化完成");
    }

    /**
     * 测试DDL同步端到端流程 - 新增字段
     */
    @Test
    public void testDDLSyncEndToEnd_AddColumn() {
        logger.info("开始测试DDL同步端到端流程 - 新增字段");
        
        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE test_table ADD COLUMN age INT";
        
        try {
            // 1. 解析源DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sourceDDL);
            
            // 2. 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_ADD", DDLOperationEnum.ALTER_ADD, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sourceDDL, ddlConfig.getSql());
            Assert.assertTrue("新增字段列表应包含age字段", ddlConfig.getAddedFieldNames().contains("age"));
            
            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 4. 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundAgeMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "age".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "age".equals(mapping.getTarget().getName())) {
                    foundAgeMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到age字段的映射", foundAgeMapping);
            
            logger.info("DDL同步端到端流程测试通过 - 新增字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 新增字段", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试DDL同步端到端流程 - 删除字段
     */
    @Test
    public void testDDLSyncEndToEnd_DropColumn() {
        logger.info("开始测试DDL同步端到端流程 - 删除字段");
        
        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE test_table DROP COLUMN name";
        
        try {
            // 1. 解析源DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sourceDDL);
            
            // 2. 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_DROP", DDLOperationEnum.ALTER_DROP, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sourceDDL, ddlConfig.getSql());
            Assert.assertTrue("删除字段列表应包含name字段", ddlConfig.getDroppedFieldNames().contains("name"));
            
            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 4. 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundNameMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "name".equals(mapping.getSource().getName())) {
                    foundNameMapping = true;
                    break;
                }
            }
            
            Assert.assertFalse("不应找到name字段的映射", foundNameMapping);
            
            logger.info("DDL同步端到端流程测试通过 - 删除字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 删除字段", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试DDL同步端到端流程 - 修改字段
     */
    @Test
    public void testDDLSyncEndToEnd_ModifyColumn() {
        logger.info("开始测试DDL同步端到端流程 - 修改字段");
        
        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE test_table MODIFY COLUMN name VARCHAR(200)";
        
        try {
            // 1. 解析源DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sourceDDL);
            
            // 2. 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_MODIFY", DDLOperationEnum.ALTER_MODIFY, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sourceDDL, ddlConfig.getSql());
            Assert.assertTrue("修改字段列表应包含name字段", ddlConfig.getModifiedFieldNames().contains("name"));
            
            // 3. 更新字段映射（修改字段属性不会影响字段映射关系）
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 4. 验证字段映射仍然存在
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundNameMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "name".equals(mapping.getTarget().getName())) {
                    foundNameMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到name字段的映射", foundNameMapping);
            
            logger.info("DDL同步端到端流程测试通过 - 修改字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 修改字段", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试DDL同步端到端流程 - 重命名字段
     */
    @Test
    public void testDDLSyncEndToEnd_ChangeColumn() {
        logger.info("开始测试DDL同步端到端流程 - 重命名字段");
        
        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE test_table CHANGE COLUMN name full_name VARCHAR(100)";
        
        try {
            // 1. 解析源DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sourceDDL);
            
            // 2. 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_CHANGE", DDLOperationEnum.ALTER_CHANGE, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", sourceDDL, ddlConfig.getSql());
            Assert.assertTrue("变更字段映射应包含name到full_name的映射", 
                ddlConfig.getChangedFieldNames().containsKey("name") && 
                "full_name".equals(ddlConfig.getChangedFieldNames().get("name")));
            
            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 4. 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundOldMapping = false;
            boolean foundNewMapping = false;
            
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "full_name".equals(mapping.getTarget().getName())) {
                    foundNewMapping = true;
                }
                if (mapping.getSource() != null && "name".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "name".equals(mapping.getTarget().getName())) {
                    foundOldMapping = true;
                }
            }
            
            Assert.assertTrue("应找到name到full_name的字段映射", foundNewMapping);
            Assert.assertFalse("不应找到name到name的旧字段映射", foundOldMapping);
            
            logger.info("DDL同步端到端流程测试通过 - 重命名字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 重命名字段", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试防循环机制
     */
    @Test
    public void testAntiLoopMechanism() {
        logger.info("开始测试防循环机制");
        
        String sourceDDL = "ALTER TABLE test_table ADD COLUMN test_col VARCHAR(50)";
        String expectedTargetDDL = "/*dbs*/ ALTER TABLE test_table ADD COLUMN test_col VARCHAR(50)";
        
        try {
            // 模拟DDL解析和转换过程
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, sourceDDL);
            
            // 验证生成的目标DDL包含防循环标识
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertTrue("目标DDL应包含防循环标识", ddlConfig.getSql().startsWith("/*dbs*/"));
            
            logger.info("防循环机制测试通过 - 生成的DDL: {}", ddlConfig.getSql());
        } catch (Exception e) {
            logger.error("防循环机制测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }
}