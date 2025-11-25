package org.dbsyncer.web.integration;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.web.Application;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * MySQL到MySQL的DDL同步集成测试
 * 全面测试MySQL之间DDL同步的端到端功能，包括解析、转换和执行
 * 覆盖场景：
 * - ADD COLUMN: 基础添加、带位置（FIRST/AFTER）、带默认值、带约束
 * - DROP COLUMN: 删除字段
 * - MODIFY COLUMN: 修改类型、修改长度、修改约束
 * - CHANGE COLUMN: 重命名字段、重命名并修改类型
 * - 异常处理
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class DDLMysqlIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DDLMysqlIntegrationTest.class);

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;
    private static TestDatabaseManager testDatabaseManager;

    private String sourceConnectorId;
    private String targetConnectorId;
    private String mappingId;
    private String metaId;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境
        String initSql = loadSqlScript("ddl/init-test-data.sql");
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("DDL同步集成测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理DDL同步集成测试环境");

        try {
            // 清理测试环境
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("DDL同步集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        // 创建Connector
        sourceConnectorId = createConnector("MySQL源连接器", sourceConfig);
        targetConnectorId = createConnector("MySQL目标连接器", targetConfig);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("DDL同步集成测试用例环境初始化完成");
    }

    @After
    public void tearDown() {
        // 停止并清理Mapping
        try {
            if (mappingId != null) {
                try {
                    mappingService.stop(mappingId);
                } catch (Exception e) {
                    // 可能已经停止，忽略
                }
                mappingService.remove(mappingId);
            }
        } catch (Exception e) {
            logger.warn("清理Mapping失败", e);
        }

        // 清理Connector
        try {
            if (sourceConnectorId != null) {
                connectorService.remove(sourceConnectorId);
            }
            if (targetConnectorId != null) {
                connectorService.remove(targetConnectorId);
            }
        } catch (Exception e) {
            logger.warn("清理Connector失败", e);
        }

        // 重置表结构
        resetDatabaseTableStructure();
    }

    /**
     * 重置数据库表结构到初始状态
     */
    private void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            String resetSql = loadSqlScript("ddl/reset-test-table.sql");
            if (resetSql == null || resetSql.trim().isEmpty()) {
                resetSql = loadSqlScript("ddl/init-test-data.sql");
            }
            if (resetSql != null && !resetSql.trim().isEmpty()) {
                testDatabaseManager.resetTableStructure(resetSql, resetSql);
                logger.debug("测试数据库表结构重置完成");
            }
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    // ==================== ADD COLUMN 测试场景 ====================

    /**
     * 测试ADD COLUMN - 基础添加字段
     */
    @Test
    public void testAddColumn_Basic() throws Exception {
        logger.info("开始测试ADD COLUMN - 基础添加字段");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN age INT";

        // 启动Mapping
        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        // 验证字段映射已更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundAgeMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "age".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "age".equals(fm.getTarget().getName()));

        assertTrue("应找到age字段的映射", foundAgeMapping);

        // 验证目标数据库中字段存在
        verifyFieldExistsInTargetDatabase("age", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN基础测试通过");
    }

    /**
     * 测试ADD COLUMN - 带AFTER子句指定位置
     */
    @Test
    public void testAddColumn_WithAfter() throws Exception {
        logger.info("开始测试ADD COLUMN - 带AFTER子句");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN email VARCHAR(100) AFTER name";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundEmailMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "email".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "email".equals(fm.getTarget().getName()));

        assertTrue("应找到email字段的映射", foundEmailMapping);
        verifyFieldExistsInTargetDatabase("email", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN带AFTER子句测试通过");
    }

    /**
     * 测试ADD COLUMN - 带FIRST子句指定位置
     */
    @Test
    public void testAddColumn_WithFirst() throws Exception {
        logger.info("开始测试ADD COLUMN - 带FIRST子句");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN priority INT FIRST";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPriorityMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "priority".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "priority".equals(fm.getTarget().getName()));

        assertTrue("应找到priority字段的映射", foundPriorityMapping);
        verifyFieldExistsInTargetDatabase("priority", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN带FIRST子句测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN status VARCHAR(20) DEFAULT 'active'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundStatusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "status".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "status".equals(fm.getTarget().getName()));

        assertTrue("应找到status字段的映射", foundStatusMapping);
        verifyFieldExistsInTargetDatabase("status", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN带默认值测试通过");
    }

    /**
     * 测试ADD COLUMN - 带NOT NULL约束
     */
    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN phone VARCHAR(20) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPhoneMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));

        assertTrue("应找到phone字段的映射", foundPhoneMapping);
        verifyFieldExistsInTargetDatabase("phone", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN带NOT NULL约束测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值和NOT NULL约束
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值和NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN created_by VARCHAR(50) NOT NULL DEFAULT 'system'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCreatedByMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "created_by".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "created_by".equals(fm.getTarget().getName()));

        assertTrue("应找到created_by字段的映射", foundCreatedByMapping);
        verifyFieldExistsInTargetDatabase("created_by", "ddlTestTable", targetConfig);

        logger.info("ADD COLUMN带默认值和NOT NULL约束测试通过");
    }

    // ==================== DROP COLUMN 测试场景 ====================

    /**
     * 测试DROP COLUMN - 删除字段
     */
    @Test
    public void testDropColumn_Basic() throws Exception {
        logger.info("开始测试DROP COLUMN - 删除字段");

        String sourceDDL = "ALTER TABLE ddlTestTable DROP COLUMN name";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()));

        assertFalse("不应找到name字段的映射", foundNameMapping);
        verifyFieldNotExistsInTargetDatabase("name", "ddlTestTable", targetConfig);

        logger.info("DROP COLUMN测试通过");
    }

    // ==================== MODIFY COLUMN 测试场景 ====================

    /**
     * 测试MODIFY COLUMN - 修改字段长度
     */
    @Test
    public void testModifyColumn_ChangeLength() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 修改字段长度");

        String sourceDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN name VARCHAR(100)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "name".equals(fm.getTarget().getName()));

        assertTrue("应找到name字段的映射", foundNameMapping);

        logger.info("MODIFY COLUMN修改长度测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 修改字段类型
     */
    @Test
    public void testModifyColumn_ChangeType() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 修改字段类型");

        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestTable ADD COLUMN count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        Thread.sleep(3000);

        String sourceDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN count_num BIGINT";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCountNumMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "count_num".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "count_num".equals(fm.getTarget().getName()));

        assertTrue("应找到count_num字段的映射", foundCountNumMapping);

        logger.info("MODIFY COLUMN修改类型测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 添加NOT NULL约束
     */
    @Test
    public void testModifyColumn_AddNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 添加NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN name VARCHAR(50) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "name".equals(fm.getTarget().getName()));

        assertTrue("应找到name字段的映射", foundNameMapping);

        logger.info("MODIFY COLUMN添加NOT NULL约束测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 移除NOT NULL约束（允许NULL）
     */
    @Test
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 移除NOT NULL约束");

        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN name VARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(setNotNullDDL, sourceConfig);
        Thread.sleep(3000);

        String sourceDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN name VARCHAR(50) NULL";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "name".equals(fm.getTarget().getName()));

        assertTrue("应找到name字段的映射", foundNameMapping);

        logger.info("MODIFY COLUMN移除NOT NULL约束测试通过");
    }

    // ==================== CHANGE COLUMN 测试场景 ====================

    /**
     * 测试CHANGE COLUMN - 重命名字段（仅重命名，不修改类型）
     */
    @Test
    public void testChangeColumn_RenameOnly() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名字段");

        String sourceDDL = "ALTER TABLE ddlTestTable CHANGE COLUMN name full_name VARCHAR(50)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "full_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "full_name".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "name".equals(fm.getTarget().getName()));

        assertTrue("应找到full_name到full_name的字段映射", foundNewMapping);
        assertTrue("不应找到name到name的旧字段映射", notFoundOldMapping);

        logger.info("CHANGE COLUMN重命名字段测试通过");
    }

    /**
     * 测试CHANGE COLUMN - 重命名并修改类型
     */
    @Test
    public void testChangeColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名并修改类型");

        // 先添加一个VARCHAR字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestTable ADD COLUMN description VARCHAR(100)";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        Thread.sleep(3000);

        String sourceDDL = "ALTER TABLE ddlTestTable CHANGE COLUMN description desc_text TEXT";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "desc_text".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "desc_text".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "description".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "description".equals(fm.getTarget().getName()));

        assertTrue("应找到desc_text到desc_text的字段映射", foundNewMapping);
        assertTrue("不应找到description到description的旧字段映射", notFoundOldMapping);

        logger.info("CHANGE COLUMN重命名并修改类型测试通过");
    }

    /**
     * 测试CHANGE COLUMN - 重命名并修改长度和约束
     */
    @Test
    public void testChangeColumn_RenameAndModifyLengthAndConstraint() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名并修改长度和约束");

        String sourceDDL = "ALTER TABLE ddlTestTable CHANGE COLUMN name user_name VARCHAR(100) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "user_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "user_name".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "name".equals(fm.getTarget().getName()));

        assertTrue("应找到user_name到user_name的字段映射", foundNewMapping);
        assertTrue("不应找到name到name的旧字段映射", notFoundOldMapping);

        logger.info("CHANGE COLUMN重命名并修改长度和约束测试通过");
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Connector
     */
    private String createConnector(String name, DatabaseConfig config) {
        Map<String, String> params = new HashMap<>();
        params.put("name", name);
        params.put("connectorType", determineConnectorType(config));
        params.put("url", config.getUrl());
        params.put("username", config.getUsername());
        params.put("password", config.getPassword());
        params.put("driverClassName", config.getDriverClassName());
        if (config.getSchema() != null) {
            params.put("schema", config.getSchema());
        }
        return connectorService.add(params);
    }

    /**
     * 创建Mapping和TableGroup
     */
    private String createMapping() {
        Map<String, String> params = new HashMap<>();
        params.put("name", "MySQL到MySQL测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "1"); // 增量同步
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        // 创建TableGroup JSON
        Map<String, Object> tableGroup = new HashMap<>();
        tableGroup.put("sourceTable", "ddlTestTable");
        tableGroup.put("targetTable", "ddlTestTable");

        List<Map<String, String>> fieldMappings = new ArrayList<>();
        Map<String, String> idMapping = new HashMap<>();
        idMapping.put("source", "id");
        idMapping.put("target", "id");
        fieldMappings.add(idMapping);

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("source", "name");
        nameMapping.put("target", "name");
        fieldMappings.add(nameMapping);

        tableGroup.put("fieldMapping", fieldMappings);

        List<Map<String, Object>> tableGroups = new ArrayList<>();
        tableGroups.add(tableGroup);

        params.put("tableGroups", org.dbsyncer.common.util.JsonUtil.objToJson(tableGroups));

        return mappingService.add(params);
    }

    /**
     * 执行DDL到源数据库
     */
    private void executeDDLToSourceDatabase(String sql, DatabaseConfig config) {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            databaseTemplate.execute(sql);
            return null;
        });
    }

    /**
     * 验证目标数据库中字段是否存在
     */
    private void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) {
        ConnectorInstance<DatabaseConfig, ?> instance = connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertTrue(String.format("目标数据库表 %s 应包含字段 %s", tableName, fieldName), exists);
    }

    /**
     * 验证目标数据库中字段是否不存在
     */
    private void verifyFieldNotExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) {
        ConnectorInstance<DatabaseConfig, ?> instance = connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertFalse(String.format("目标数据库表 %s 不应包含字段 %s", tableName, fieldName), exists);
    }

    /**
     * 从URL推断连接器类型
     */
    private static String determineConnectorType(DatabaseConfig config) {
        String url = config.getUrl();
        if (url == null) {
            return "MySQL";
        }
        String urlLower = url.toLowerCase();
        if (urlLower.contains("mysql")) {
            return "MySQL";
        } else if (urlLower.contains("sqlserver") || urlLower.contains("jdbc:sqlserver")) {
            return "SqlServer";
        }
        return "MySQL";
    }

    /**
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = DDLMysqlIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                sourceConfig = createDefaultMySQLConfig();
                targetConfig = createDefaultMySQLConfig();
                return;
            }
            props.load(input);
        }

        // 创建源数据库配置
        sourceConfig = new DatabaseConfig();
        sourceConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/source_db"));
        sourceConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        sourceConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        sourceConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));

        // 创建目标数据库配置
        targetConfig = new DatabaseConfig();
        targetConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/target_db"));
        targetConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        targetConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        targetConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));
    }

    /**
     * 创建默认的MySQL配置
     */
    private static DatabaseConfig createDefaultMySQLConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false");
        config.setUsername("root");
        config.setPassword("123");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }

    /**
     * 加载SQL脚本文件
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = DDLMysqlIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            if (input == null) {
                logger.warn("未找到SQL脚本文件: {}", resourcePath);
                return "";
            }

            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            logger.error("加载SQL脚本文件失败: {}", resourcePath, e);
            return "";
        }
    }
}

