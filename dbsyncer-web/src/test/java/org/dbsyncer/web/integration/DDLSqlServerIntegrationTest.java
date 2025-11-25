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
 * SQL Server到SQL Server的DDL同步集成测试
 * 全面测试SQL Server之间DDL同步的端到端功能，包括解析、转换和执行
 * 覆盖场景：
 * - ADD COLUMN: 基础添加、带默认值、带约束、带NULL/NOT NULL
 * - DROP COLUMN: 删除字段
 * - ALTER COLUMN: 修改类型、修改长度、修改约束（NULL/NOT NULL）
 * - 异常处理
 * 
 * 注意：
 * - SQL Server不支持CHANGE COLUMN语法，重命名字段需要使用sp_rename存储过程
 * - sp_rename不是标准的ALTER TABLE语句，无法通过JSQLParser解析
 * - 字段重命名功能应在异构数据库测试中验证（如MySQL到SQL Server）
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class DDLSqlServerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DDLSqlServerIntegrationTest.class);

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
        logger.info("开始初始化SQL Server到SQL Server的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境
        String initSql = loadSqlScript("ddl/init-test-data.sql");
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("SQL Server到SQL Server的DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server到SQL Server的DDL同步测试环境");

        try {
            // 清理测试环境
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("SQL Server到SQL Server的DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        // 创建Connector
        sourceConnectorId = createConnector("SQL Server源连接器", sourceConfig);
        targetConnectorId = createConnector("SQL Server目标连接器", targetConfig);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("SQL Server到SQL Server的DDL同步测试用例环境初始化完成");
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
     * SQL Server 测试使用 ddlTestEmployee 表，需要专门的重置逻辑
     */
    private void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            // SQL Server 专用的重置 SQL：删除并重建 ddlTestEmployee 表
            String resetSql = 
                "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                "CREATE TABLE ddlTestEmployee (\n" +
                "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                "    first_name NVARCHAR(50) NOT NULL,\n" +
                "    last_name NVARCHAR(50),\n" +
                "    department NVARCHAR(100),\n" +
                "    created_at DATETIME2 DEFAULT GETDATE()\n" +
                ");\n" +
                "DELETE FROM ddlTestEmployee;";
            
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

        // SQL Server ADD 语句（注意SQL Server不需要COLUMN关键字）
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundSalaryMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "salary".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "salary".equals(fm.getTarget().getName()));

        assertTrue("应找到salary字段的映射", foundSalaryMapping);
        verifyFieldExistsInTargetDatabase("salary", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN基础测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD status NVARCHAR(20) DEFAULT 'active'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundStatusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "status".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "status".equals(fm.getTarget().getName()));

        assertTrue("应找到status字段的映射", foundStatusMapping);
        verifyFieldExistsInTargetDatabase("status", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带默认值测试通过");
    }

    /**
     * 测试ADD COLUMN - 带NOT NULL约束
     */
    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD phone NVARCHAR(20) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPhoneMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));

        assertTrue("应找到phone字段的映射", foundPhoneMapping);
        verifyFieldExistsInTargetDatabase("phone", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带NOT NULL约束测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值和NOT NULL约束
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值和NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD created_by NVARCHAR(50) NOT NULL DEFAULT 'system'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCreatedByMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "created_by".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "created_by".equals(fm.getTarget().getName()));

        assertTrue("应找到created_by字段的映射", foundCreatedByMapping);
        verifyFieldExistsInTargetDatabase("created_by", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带默认值和NOT NULL约束测试通过");
    }

    // ==================== DROP COLUMN 测试场景 ====================

    /**
     * 测试DROP COLUMN - 删除字段
     */
    @Test
    public void testDropColumn_Basic() throws Exception {
        logger.info("开始测试DROP COLUMN - 删除字段");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee DROP COLUMN department";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundDepartmentMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "department".equals(fm.getSource().getName()));

        assertFalse("不应找到department字段的映射", foundDepartmentMapping);
        verifyFieldNotExistsInTargetDatabase("department", "ddlTestEmployee", targetConfig);

        logger.info("DROP COLUMN测试通过");
    }

    // ==================== ALTER COLUMN 测试场景 ====================

    /**
     * 测试ALTER COLUMN - 修改字段长度
     */
    @Test
    public void testAlterColumn_ChangeLength() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段长度");

        // SQL Server使用ALTER COLUMN而不是MODIFY COLUMN
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("ALTER COLUMN修改长度测试通过");
    }

    /**
     * 测试ALTER COLUMN - 修改字段类型
     */
    @Test
    public void testAlterColumn_ChangeType() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段类型");

        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        Thread.sleep(3000);

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN count_num BIGINT";

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCountNumMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "count_num".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "count_num".equals(fm.getTarget().getName()));

        assertTrue("应找到count_num字段的映射", foundCountNumMapping);

        logger.info("ALTER COLUMN修改类型测试通过");
    }

    /**
     * 测试ALTER COLUMN - 添加NOT NULL约束
     */
    @Test
    public void testAlterColumn_AddNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 添加NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN last_name NVARCHAR(50) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundLastNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "last_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "last_name".equals(fm.getTarget().getName()));

        assertTrue("应找到last_name字段的映射", foundLastNameMapping);

        logger.info("ALTER COLUMN添加NOT NULL约束测试通过");
    }

    /**
     * 测试ALTER COLUMN - 移除NOT NULL约束（允许NULL）
     */
    @Test
    public void testAlterColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 移除NOT NULL约束");

        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(setNotNullDDL, sourceConfig);
        Thread.sleep(3000);

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NULL";

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(3000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("ALTER COLUMN移除NOT NULL约束测试通过");
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
        params.put("name", "SQL Server到SQL Server测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "1"); // 增量同步
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        // 创建TableGroup JSON
        Map<String, Object> tableGroup = new HashMap<>();
        tableGroup.put("sourceTable", "ddlTestEmployee");
        tableGroup.put("targetTable", "ddlTestEmployee");

        List<Map<String, String>> fieldMappings = new ArrayList<>();
        Map<String, String> idMapping = new HashMap<>();
        idMapping.put("source", "id");
        idMapping.put("target", "id");
        fieldMappings.add(idMapping);

        Map<String, String> firstNameMapping = new HashMap<>();
        firstNameMapping.put("source", "first_name");
        firstNameMapping.put("target", "first_name");
        fieldMappings.add(firstNameMapping);

        Map<String, String> lastNameMapping = new HashMap<>();
        lastNameMapping.put("source", "last_name");
        lastNameMapping.put("target", "last_name");
        fieldMappings.add(lastNameMapping);

        Map<String, String> departmentMapping = new HashMap<>();
        departmentMapping.put("source", "department");
        departmentMapping.put("target", "department");
        fieldMappings.add(departmentMapping);

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
        try (InputStream input = DDLSqlServerIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                sourceConfig = createDefaultSQLServerConfig();
                targetConfig = createDefaultSQLServerConfig();
                return;
            }
            props.load(input);
        }

        // 创建源数据库配置(SQL Server)
        sourceConfig = new DatabaseConfig();
        sourceConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true"));
        sourceConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sourceConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sourceConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));

        // 创建目标数据库配置(SQL Server)
        targetConfig = new DatabaseConfig();
        targetConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=target_db;encrypt=false;trustServerCertificate=true"));
        targetConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        targetConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        targetConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    }

    /**
     * 创建默认的SQL Server配置
     */
    private static DatabaseConfig createDefaultSQLServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test;encrypt=false;trustServerCertificate=true");
        config.setUsername("sa");
        config.setPassword("123");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }

    /**
     * 加载SQL脚本文件
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = DDLSqlServerIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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

