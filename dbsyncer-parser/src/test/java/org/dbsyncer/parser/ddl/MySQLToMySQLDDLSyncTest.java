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
 * MySQL到MySQL的DDL同步测试
 * 验证同类型数据库间的DDL同步功能
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class MySQLToMySQLDDLSyncTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;

    @Before
    public void setUp() {
        ddlParser = new DDLParserImpl();
        
        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("mysql-test-tablegroup-id");
        testTableGroup.setMappingId("mysql-test-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理
        
        // 创建源表和目标表（MySQL）
        Table sourceTable = new Table();
        sourceTable.setName("user_info");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        Table targetTable = new Table();
        targetTable.setName("user_info");
        // Note: Table类中没有setSchema方法，我们只设置表名
        
        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);
        
        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();
        
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping idMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(idMapping);
        
        Field usernameSourceField = new Field("username", "VARCHAR", 12);
        Field usernameTargetField = new Field("username", "VARCHAR", 12);
        FieldMapping usernameMapping = new FieldMapping(usernameSourceField, usernameTargetField);
        fieldMappings.add(usernameMapping);
        
        Field emailSourceField = new Field("email", "VARCHAR", 12);
        Field emailTargetField = new Field("email", "VARCHAR", 12);
        FieldMapping emailMapping = new FieldMapping(emailSourceField, emailTargetField);
        fieldMappings.add(emailMapping);
        
        testTableGroup.setFieldMapping(fieldMappings);
        
        logger.info("MySQL到MySQL的DDL同步测试环境初始化完成");
    }

    /**
     * 测试MySQL新增字段同步
     */
    @Test
    public void testMySQLAddColumnSync() {
        logger.info("开始测试MySQL新增字段同步");
        
        // MySQL ADD COLUMN 语句
        String mysqlDDL = "ALTER TABLE user_info ADD COLUMN phone VARCHAR(20) AFTER email";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_ADD", DDLOperationEnum.ALTER_ADD, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", mysqlDDL, ddlConfig.getSql());
            Assert.assertTrue("新增字段列表应包含phone字段", ddlConfig.getAddedFieldNames().contains("phone"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundPhoneMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "phone".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "phone".equals(mapping.getTarget().getName())) {
                    foundPhoneMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到phone字段的映射", foundPhoneMapping);
            
            logger.info("MySQL新增字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL新增字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试MySQL删除字段同步
     */
    @Test
    public void testMySQLDropColumnSync() {
        logger.info("开始测试MySQL删除字段同步");
        
        // MySQL DROP COLUMN 语句
        String mysqlDDL = "ALTER TABLE user_info DROP COLUMN email";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_DROP", DDLOperationEnum.ALTER_DROP, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", mysqlDDL, ddlConfig.getSql());
            Assert.assertTrue("删除字段列表应包含email字段", ddlConfig.getDroppedFieldNames().contains("email"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundEmailMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "email".equals(mapping.getSource().getName())) {
                    foundEmailMapping = true;
                    break;
                }
            }
            
            Assert.assertFalse("不应找到email字段的映射", foundEmailMapping);
            
            logger.info("MySQL删除字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL删除字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试MySQL修改字段同步
     */
    @Test
    public void testMySQLModifyColumnSync() {
        logger.info("开始测试MySQL修改字段同步");
        
        // MySQL MODIFY COLUMN 语句
        String mysqlDDL = "ALTER TABLE user_info MODIFY COLUMN username VARCHAR(100) NOT NULL";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_MODIFY", DDLOperationEnum.ALTER_MODIFY, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", mysqlDDL, ddlConfig.getSql());
            Assert.assertTrue("修改字段列表应包含username字段", ddlConfig.getModifiedFieldNames().contains("username"));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射仍然存在
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundUsernameMapping = false;
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "username".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "username".equals(mapping.getTarget().getName())) {
                    foundUsernameMapping = true;
                    break;
                }
            }
            
            Assert.assertTrue("应找到username字段的映射", foundUsernameMapping);
            
            logger.info("MySQL修改字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL修改字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }

    /**
     * 测试MySQL重命名字段同步
     */
    @Test
    public void testMySQLChangeColumnSync() {
        logger.info("开始测试MySQL重命名字段同步");
        
        // MySQL CHANGE COLUMN 语句
        String mysqlDDL = "ALTER TABLE user_info CHANGE COLUMN username user_name VARCHAR(50)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);
            
            // 验证解析结果
            Assert.assertNotNull("DDL配置不应为空", ddlConfig);
            Assert.assertEquals("DDL操作类型应为ALTER_CHANGE", DDLOperationEnum.ALTER_CHANGE, ddlConfig.getDdlOperationEnum());
            Assert.assertEquals("SQL语句应匹配", mysqlDDL, ddlConfig.getSql());
            Assert.assertTrue("变更字段映射应包含username到user_name的映射", 
                ddlConfig.getChangedFieldNames().containsKey("username") && 
                "user_name".equals(ddlConfig.getChangedFieldNames().get("username")));
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);
            
            // 验证字段映射更新
            List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
            boolean foundOldMapping = false;
            boolean foundNewMapping = false;
            
            for (FieldMapping mapping : fieldMappings) {
                if (mapping.getSource() != null && "username".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "user_name".equals(mapping.getTarget().getName())) {
                    foundNewMapping = true;
                }
                if (mapping.getSource() != null && "username".equals(mapping.getSource().getName()) && 
                    mapping.getTarget() != null && "username".equals(mapping.getTarget().getName())) {
                    foundOldMapping = true;
                }
            }
            
            Assert.assertTrue("应找到username到user_name的字段映射", foundNewMapping);
            Assert.assertFalse("不应找到username到username的旧字段映射", foundOldMapping);
            
            logger.info("MySQL重命名字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL重命名字段同步测试失败", e);
            Assert.fail("测试应成功完成，但抛出异常: " + e.getMessage());
        }
    }
}