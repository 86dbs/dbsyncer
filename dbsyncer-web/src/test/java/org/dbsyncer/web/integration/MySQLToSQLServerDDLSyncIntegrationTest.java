package org.dbsyncer.web.integration;

import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.web.Application;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * MySQL到SQL Server的DDL同步集成测试
 * 全面测试MySQL到SQL Server的DDL同步功能，包括类型转换、操作转换等
 * 覆盖场景：
 * - MySQL特殊类型转换：ENUM, SET, JSON, YEAR, TINYTEXT, MEDIUMTEXT, LONGTEXT, BIT
 * - DDL操作：ADD COLUMN, MODIFY COLUMN, CHANGE COLUMN, DROP COLUMN
 * - 复杂场景：带约束字段添加、字段重命名
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class MySQLToSQLServerDDLSyncIntegrationTest extends BaseDDLIntegrationTest {

    private static DatabaseConfig mysqlConfig;
    private static DatabaseConfig sqlServerConfig;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化MySQL到SQL Server的DDL同步测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 设置基类的sourceConfig和targetConfig（用于基类方法）
        sourceConfig = mysqlConfig;
        targetConfig = sqlServerConfig;

        // 创建测试数据库管理器（第一个参数是MySQL，第二个是SQL Server）
        testDatabaseManager = new TestDatabaseManager(mysqlConfig, sqlServerConfig);

        // 初始化测试环境
        String mysqlInitSql =
                "DROP TABLE IF EXISTS ddlTestEmployee;\n" +
                        "CREATE TABLE ddlTestEmployee (\n" +
                        "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                        "    first_name VARCHAR(50) NOT NULL\n" +
                        ");";

        String sqlServerInitSql =
                "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                        "CREATE TABLE ddlTestEmployee (\n" +
                        "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                        "    first_name NVARCHAR(50) NOT NULL\n" +
                        ");";

        testDatabaseManager.initializeTestEnvironment(mysqlInitSql, sqlServerInitSql);

        logger.info("MySQL到SQL Server的DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理MySQL到SQL Server的DDL同步测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            // 源数据库是MySQL，目标数据库是SQL Server，需要分别加载对应的清理脚本
            String sourceCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "mysql", MySQLToSQLServerDDLSyncIntegrationTest.class);
            String targetCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "sqlserver", MySQLToSQLServerDDLSyncIntegrationTest.class);
            testDatabaseManager.cleanupTestEnvironment(sourceCleanupSql, targetCleanupSql);
            logger.info("MySQL到SQL Server的DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        // 创建Connector
        sourceConnectorId = createConnector(getSourceConnectorName(), mysqlConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), sqlServerConfig, false);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("MySQL到SQL Server的DDL同步测试用例环境初始化完成");
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
     * 重置数据库表结构到初始状态（覆盖基类方法，使用异构数据库的特殊逻辑）
     */
    @Override
    protected void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            String mysqlResetSql =
                    "DROP TABLE IF EXISTS ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                            "    first_name VARCHAR(50) NOT NULL\n" +
                            ");";

            String sqlServerResetSql =
                    "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                            "    first_name NVARCHAR(50) NOT NULL\n" +
                            ");";

            testDatabaseManager.resetTableStructure(mysqlResetSql, sqlServerResetSql);
            logger.debug("测试数据库表结构重置完成");
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    // ==================== MySQL特殊类型转换测试 ====================

    @Test
    public void testAddColumn_ENUMType() throws Exception {
        logger.info("开始测试ENUM类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status ENUM('active','inactive','pending')";
        testDDLConversion(mysqlDDL, "status");
    }

    @Test
    public void testAddColumn_SETType() throws Exception {
        logger.info("开始测试SET类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN tags SET('tag1','tag2','tag3')";
        testDDLConversion(mysqlDDL, "tags");
    }

    @Test
    public void testAddColumn_JSONType() throws Exception {
        logger.info("开始测试JSON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN metadata JSON";
        testDDLConversion(mysqlDDL, "metadata");
    }

    @Test
    public void testAddColumn_YEARType() throws Exception {
        logger.info("开始测试YEAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_year YEAR";
        testDDLConversion(mysqlDDL, "birth_year");
    }

    @Test
    public void testAddColumn_GEOMETRYType() throws Exception {
        logger.info("开始测试GEOMETRY类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN location GEOMETRY";
        testDDLConversion(mysqlDDL, "location");
    }

    @Test
    public void testAddColumn_POINTType() throws Exception {
        logger.info("开始测试POINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN coordinates POINT";
        testDDLConversion(mysqlDDL, "coordinates");
    }

    @Test
    public void testAddColumn_LINESTRINGType() throws Exception {
        logger.info("开始测试LINESTRING类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN route LINESTRING";
        testDDLConversion(mysqlDDL, "route");
    }

    @Test
    public void testAddColumn_POLYGONType() throws Exception {
        logger.info("开始测试POLYGON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN area POLYGON";
        testDDLConversion(mysqlDDL, "area");
    }

    @Test
    public void testAddColumn_MULTIPOINTType() throws Exception {
        logger.info("开始测试MULTIPOINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN points MULTIPOINT";
        testDDLConversion(mysqlDDL, "points");
    }

    @Test
    public void testAddColumn_MULTILINESTRINGType() throws Exception {
        logger.info("开始测试MULTILINESTRING类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN routes MULTILINESTRING";
        testDDLConversion(mysqlDDL, "routes");
    }

    @Test
    public void testAddColumn_MULTIPOLYGONType() throws Exception {
        logger.info("开始测试MULTIPOLYGON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN regions MULTIPOLYGON";
        testDDLConversion(mysqlDDL, "regions");
    }

    @Test
    public void testAddColumn_GEOMETRYCOLLECTIONType() throws Exception {
        logger.info("开始测试GEOMETRYCOLLECTION类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN collection GEOMETRYCOLLECTION";
        testDDLConversion(mysqlDDL, "collection");
    }

    @Test
    public void testAddColumn_TINYTEXTType() throws Exception {
        logger.info("开始测试TINYTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN short_text TINYTEXT";
        testDDLConversion(mysqlDDL, "short_text");
    }

    @Test
    public void testAddColumn_MEDIUMTEXTType() throws Exception {
        logger.info("开始测试MEDIUMTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN medium_text MEDIUMTEXT";
        testDDLConversion(mysqlDDL, "medium_text");
    }

    @Test
    public void testAddColumn_LONGTEXTType() throws Exception {
        logger.info("开始测试LONGTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN long_text LONGTEXT";
        testDDLConversion(mysqlDDL, "long_text");
    }

    @Test
    public void testAddColumn_BITType() throws Exception {
        logger.info("开始测试BIT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN is_verified BIT(1)";
        testDDLConversion(mysqlDDL, "is_verified");
    }

    // ==================== MySQL基础类型转换测试 ====================

    @Test
    public void testAddColumn_VARCHARType() throws Exception {
        logger.info("开始测试VARCHAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN last_name VARCHAR(100)";
        testDDLConversion(mysqlDDL, "last_name");
    }

    @Test
    public void testAddColumn_CHARType() throws Exception {
        logger.info("开始测试CHAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN code CHAR(10)";
        testDDLConversion(mysqlDDL, "code");
    }

    @Test
    public void testAddColumn_INTType() throws Exception {
        logger.info("开始测试INT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN age INT";
        testDDLConversion(mysqlDDL, "age");
    }

    @Test
    public void testAddColumn_BIGINTType() throws Exception {
        logger.info("开始测试BIGINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN count_num BIGINT";
        testDDLConversion(mysqlDDL, "count_num");
    }

    @Test
    public void testAddColumn_DECIMALType() throws Exception {
        logger.info("开始测试DECIMAL类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN price DECIMAL(10,2)";
        testDDLConversion(mysqlDDL, "price");
    }

    @Test
    public void testAddColumn_DATEType() throws Exception {
        logger.info("开始测试DATE类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_date DATE";
        testDDLConversion(mysqlDDL, "birth_date");
    }

    @Test
    public void testAddColumn_TIMEType() throws Exception {
        logger.info("开始测试TIME类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN work_time TIME";
        testDDLConversion(mysqlDDL, "work_time");
    }

    @Test
    public void testAddColumn_DATETIMEType() throws Exception {
        logger.info("开始测试DATETIME类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN updated_at DATETIME";
        testDDLConversion(mysqlDDL, "updated_at");
    }

    @Test
    public void testAddColumn_TIMESTAMPType() throws Exception {
        logger.info("开始测试TIMESTAMP类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN created_at TIMESTAMP";
        testDDLConversion(mysqlDDL, "created_at");
    }

    // ==================== DDL操作测试 ====================

    @Test
    public void testModifyColumn_ChangeLength() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 修改长度");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(100)";
        testDDLConversion(mysqlDDL, "first_name");
    }

    @Test
    public void testModifyColumn_AddNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 添加NOT NULL约束");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NOT NULL";
        testDDLConversion(mysqlDDL, "first_name");
    }

    @Test
    public void testModifyColumn_ChangeType() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 修改类型");
        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, mysqlConfig);
        Thread.sleep(3000);

        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN count_num BIGINT";
        testDDLConversion(mysqlDDL, "count_num");
    }

    @Test
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(setNotNullDDL, mysqlConfig);
        Thread.sleep(3000);

        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NULL";
        testDDLConversion(mysqlDDL, "first_name");
    }

    @Test
    public void testChangeColumn_RenameOnly() throws Exception {
        logger.info("开始测试CHANGE COLUMN操作 - 仅重命名");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN first_name given_name VARCHAR(50)";
        testDDLConversion(mysqlDDL, "given_name");
    }

    @Test
    public void testChangeColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试CHANGE COLUMN操作 - 重命名并修改类型");
        // 先添加一个字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN description VARCHAR(100)";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, mysqlConfig);
        Thread.sleep(3000);

        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN description desc_text TEXT";
        testDDLConversion(mysqlDDL, "desc_text");
    }

    @Test
    public void testChangeColumn_RenameAndModifyLengthAndConstraint() throws Exception {
        logger.info("开始测试CHANGE COLUMN操作 - 重命名并修改长度和约束");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN first_name given_name VARCHAR(100) NOT NULL";
        testDDLConversion(mysqlDDL, "given_name");
    }

    @Test
    public void testDropColumn() throws Exception {
        logger.info("开始测试DROP COLUMN操作");
        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name", "first_name");
    }

    // ==================== 复杂场景测试 ====================

    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试带默认值的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status VARCHAR(20) DEFAULT 'active'";
        testDDLConversion(mysqlDDL, "status");
    }

    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试带NOT NULL约束的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN phone VARCHAR(20) NOT NULL";
        testDDLConversion(mysqlDDL, "phone");
    }

    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN email VARCHAR(100) NOT NULL DEFAULT 'unknown@example.com'";
        testDDLConversion(mysqlDDL, "email");
    }

    @Test
    public void testAddColumn_WithAfter() throws Exception {
        logger.info("开始测试带AFTER子句的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN last_name VARCHAR(50) AFTER first_name";
        testDDLConversion(mysqlDDL, "last_name");
    }

    @Test
    public void testAddColumn_WithFirst() throws Exception {
        logger.info("开始测试带FIRST子句的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN priority INT FIRST";
        testDDLConversion(mysqlDDL, "priority");
    }

    @Test
    public void testAddMultipleColumns() throws Exception {
        logger.info("开始测试多字段同时添加");
        // MySQL语法：每个字段都需要ADD COLUMN关键字
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN salary DECIMAL(10,2), ADD COLUMN bonus DECIMAL(8,2)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射是否更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundSalaryMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "salary".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "salary".equals(fm.getTarget().getName()));
        assertTrue("应找到salary字段的映射", foundSalaryMapping);

        boolean foundBonusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "bonus".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "bonus".equals(fm.getTarget().getName()));
        assertTrue("应找到bonus字段的映射", foundBonusMapping);

        // 验证目标数据库中两个字段都存在
        verifyFieldExistsInTargetDatabase("salary", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldExistsInTargetDatabase("bonus", tableGroup.getTargetTable().getName(), sqlServerConfig);

        logger.info("多字段添加测试通过 - salary和bonus字段都已正确转换");
    }

    // ==================== 通用测试方法 ====================

    /**
     * 执行DDL转换并验证结果
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName) throws Exception {
        // 启动Mapping
        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射是否更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
        boolean isChangeOperation = sourceDDL.toUpperCase().contains("CHANGE");

        if (isAddOperation || isChangeOperation) {
            boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()) &&
                            fm.getTarget() != null && expectedFieldName.equals(fm.getTarget().getName()));
            assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);

            // 验证目标数据库中字段是否存在
            verifyFieldExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), sqlServerConfig);
        } else {
            boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
            assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
        }

        logger.info("DDL转换测试通过 - 字段: {}", expectedFieldName);
    }

    /**
     * 测试DDL DROP操作
     */
    private void testDDLDropOperation(String sourceDDL, String expectedFieldName) throws Exception {
        // 启动Mapping
        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射是否已移除
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
        assertFalse("应移除字段 " + expectedFieldName + " 的映射", foundFieldMapping);

        // 验证目标数据库中字段是否已被删除
        verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), sqlServerConfig);

        logger.info("DDL DROP操作测试通过 - 字段: {}", expectedFieldName);
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Mapping和TableGroup（覆盖基类方法，使用特殊的JSON格式）
     */
    @Override
    protected String createMapping() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", getMappingName());
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment"); // 增量同步
        params.put("incrementStrategy", getIncrementStrategy());
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        // 创建TableGroup JSON（这个测试类使用特殊的JSON格式）
        Map<String, Object> tableGroup = new HashMap<>();
        tableGroup.put("sourceTable", getSourceTableName());
        tableGroup.put("targetTable", getTargetTableName());

        List<Map<String, String>> fieldMappings = new ArrayList<>();
        for (String mapping : getInitialFieldMappings()) {
            String[] parts = mapping.split("\\|");
            if (parts.length == 2) {
                Map<String, String> fieldMapping = new HashMap<>();
                fieldMapping.put("source", parts[0]);
                fieldMapping.put("target", parts[1]);
                fieldMappings.add(fieldMapping);
            }
        }

        tableGroup.put("fieldMapping", fieldMappings);

        List<Map<String, Object>> tableGroups = new ArrayList<>();
        tableGroups.add(tableGroup);

        params.put("tableGroups", org.dbsyncer.common.util.JsonUtil.objToJson(tableGroups));

        String mappingId = mappingService.add(params);

        // 创建后需要编辑一次以正确设置增量同步配置
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment");
        editParams.put("incrementStrategy", getIncrementStrategy());
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        return mappingId;
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
        Properties props = new Properties();
        try (InputStream input = MySQLToSQLServerDDLSyncIntegrationTest.class.getClassLoader()
                .getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                mysqlConfig = createDefaultMySQLConfig();
                sqlServerConfig = createDefaultSQLServerConfig();
                return;
            }
            props.load(input);
        }

        mysqlConfig = new DatabaseConfig();
        mysqlConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/source_db"));
        mysqlConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        mysqlConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        mysqlConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));

        sqlServerConfig = new DatabaseConfig();
        sqlServerConfig.setUrl(props.getProperty("test.db.sqlserver.url",
                "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=target_db;encrypt=false;trustServerCertificate=true"));
        sqlServerConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sqlServerConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sqlServerConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return MySQLToSQLServerDDLSyncIntegrationTest.class;
    }

    @Override
    protected String getSourceConnectorName() {
        return "MySQL源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "SQL Server目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "MySQL到SQL Server测试Mapping";
    }

    @Override
    protected String getSourceTableName() {
        return "ddlTestEmployee";
    }

    @Override
    protected String getTargetTableName() {
        return "ddlTestEmployee";
    }

    @Override
    protected List<String> getInitialFieldMappings() {
        List<String> fieldMappingList = new ArrayList<>();
        fieldMappingList.add("id|id");
        fieldMappingList.add("first_name|first_name");
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        if (isSource) {
            return "MySQL"; // 源是 MySQL
        } else {
            return "SqlServer"; // 目标是 SQL Server
        }
    }

    @Override
    protected String getIncrementStrategy() {
        return "Log"; // MySQL 使用 binlog
    }

    @Override
    protected String getDatabaseType(boolean isSource) {
        if (isSource) {
            return "mysql"; // 源是 MySQL
        } else {
            return "sqlserver"; // 目标是 SQL Server
        }
    }
}

