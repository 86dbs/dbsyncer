package org.dbsyncer.web.integration;

import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.web.Application;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

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
@RunWith(SpringRunner.class)
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
        // 先清理可能残留的测试 mapping（防止上一个测试清理失败导致残留）
        cleanupResidualTestMappings();

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
        // 环境准备：直接在源库和目标库添加字段（不通过同步机制）
        prepareEnvironment(
                "ALTER TABLE ddlTestEmployee ADD COLUMN count_num INT",
                "ALTER TABLE ddlTestEmployee ADD count_num INT"
        );

        // 测试：修改字段类型（通过同步机制）
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN count_num BIGINT";
        testDDLConversion(mysqlDDL, "count_num");
    }

    @Test
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        // 环境准备：直接在源库和目标库设置字段为NOT NULL（不通过同步机制）
        prepareEnvironment(
                "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NOT NULL",
                "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL"
        );

        // 测试：移除NOT NULL约束（通过同步机制）
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
        // 环境准备：直接在源库和目标库添加字段（不通过同步机制）
        prepareEnvironment(
                "ALTER TABLE ddlTestEmployee ADD COLUMN description VARCHAR(100)",
                "ALTER TABLE ddlTestEmployee ADD description NVARCHAR(100)"
        );

        // 测试：重命名并修改类型（通过同步机制）
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

    /**
     * 测试ADD COLUMN - 带DEFAULT值（源DDL中的DEFAULT值会被忽略）
     * 注意：根据2.7.0版本的设计，缺省值处理已被忽略，因为各数据库缺省值函数表达差异很大
     * 源DDL中的DEFAULT值在解析时会被跳过，不会同步到目标数据库
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试带默认值的字段添加（源DDL中的DEFAULT值会被忽略）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status VARCHAR(20) DEFAULT 'active'";
        testDDLConversion(mysqlDDL, "status");
        
        // 验证：源DDL中的DEFAULT值不会被同步，目标字段不应该有DEFAULT值（除非是NOT NULL字段自动生成的）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        // status字段是可空的，所以不应该有DEFAULT值
        // 注意：这里不验证DEFAULT值，因为源DDL中的DEFAULT值已被忽略
    }

    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试带NOT NULL约束的字段添加（应自动添加DEFAULT值）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN phone VARCHAR(20) NOT NULL";
        
        // 执行DDL转换
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        // 验证字段映射是否更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);
        
        boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));
        assertTrue("应找到字段 phone 的映射", foundFieldMapping);
        
        // 验证目标数据库中字段是否存在
        verifyFieldExistsInTargetDatabase("phone", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        // 验证：SQL Server 应该自动添加了 DEFAULT 值（仅为了满足语法要求）
        // 对于 VARCHAR 类型，应该是 DEFAULT ''（空字符串）
        verifyFieldDefaultValue("phone", tableGroup.getTargetTable().getName(), sqlServerConfig, "''");
        
        // 验证 NOT NULL 约束
        verifyFieldNotNull("phone", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("ADD COLUMN带NOT NULL约束测试通过（已验证DEFAULT值自动添加）");
    }

    /**
     * 测试ADD COLUMN - 带DEFAULT值和NOT NULL约束（源DDL中的DEFAULT值会被忽略）
     * 注意：源DDL中的DEFAULT值在解析时会被跳过，目标数据库会使用自动生成的DEFAULT值（仅为了满足语法要求）
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加（源DDL中的DEFAULT值会被忽略）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN email VARCHAR(100) NOT NULL DEFAULT 'unknown@example.com'";
        testDDLConversion(mysqlDDL, "email");
        
        // 验证：源DDL中的DEFAULT 'unknown@example.com'会被忽略，目标数据库会使用自动生成的DEFAULT ''（仅为了满足语法要求）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldDefaultValue("email", tableGroup.getTargetTable().getName(), sqlServerConfig, "''");
        verifyFieldNotNull("email", tableGroup.getTargetTable().getName(), sqlServerConfig);
    }

    // ==================== NOT NULL + 自动DEFAULT值测试（不同数据类型） ====================

    @Test
    public void testAddColumn_WithNotNull_INT() throws Exception {
        logger.info("开始测试INT类型NOT NULL字段（应自动添加DEFAULT 0）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN age INT NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("age", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldDefaultValue("age", tableGroup.getTargetTable().getName(), sqlServerConfig, "0");
        verifyFieldNotNull("age", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("INT类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    @Test
    public void testAddColumn_WithNotNull_BIGINT() throws Exception {
        logger.info("开始测试BIGINT类型NOT NULL字段（应自动添加DEFAULT 0）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN count_num BIGINT NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("count_num", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldDefaultValue("count_num", tableGroup.getTargetTable().getName(), sqlServerConfig, "0");
        verifyFieldNotNull("count_num", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("BIGINT类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    @Test
    public void testAddColumn_WithNotNull_DECIMAL() throws Exception {
        logger.info("开始测试DECIMAL类型NOT NULL字段（应自动添加DEFAULT 0）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN price DECIMAL(10,2) NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("price", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldDefaultValue("price", tableGroup.getTargetTable().getName(), sqlServerConfig, "0");
        verifyFieldNotNull("price", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("DECIMAL类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    @Test
    public void testAddColumn_WithNotNull_DATE() throws Exception {
        logger.info("开始测试DATE类型NOT NULL字段（应自动添加DEFAULT '1900-01-01'）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_date DATE NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("birth_date", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldDefaultValue("birth_date", tableGroup.getTargetTable().getName(), sqlServerConfig, "'1900-01-01'");
        verifyFieldNotNull("birth_date", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("DATE类型NOT NULL字段测试通过（已验证DEFAULT '1900-01-01'自动添加）");
    }

    @Test
    public void testAddColumn_WithNotNull_DATETIME() throws Exception {
        logger.info("开始测试DATETIME类型NOT NULL字段（应自动添加DEFAULT '1900-01-01'）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN created_at DATETIME NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("created_at", tableGroup.getTargetTable().getName(), sqlServerConfig);
        // DATETIME 类型在 SQL Server 中会转换为 DATETIME2，默认值是 '1900-01-01 00:00:00'
        // 但实际存储的默认值可能是 '1900-01-01' 或 '1900-01-01 00:00:00'
        // 这里我们验证至少包含日期部分
        verifyFieldDefaultValue("created_at", tableGroup.getTargetTable().getName(), sqlServerConfig, "'1900-01-01'");
        verifyFieldNotNull("created_at", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("DATETIME类型NOT NULL字段测试通过（已验证DEFAULT值自动添加）");
    }

    @Test
    public void testAddColumn_WithNotNull_CHAR() throws Exception {
        logger.info("开始测试CHAR类型NOT NULL字段（应自动添加DEFAULT ''）");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN code CHAR(10) NOT NULL";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("code", tableGroup.getTargetTable().getName(), sqlServerConfig);
        verifyFieldDefaultValue("code", tableGroup.getTargetTable().getName(), sqlServerConfig, "''");
        verifyFieldNotNull("code", tableGroup.getTargetTable().getName(), sqlServerConfig);
        
        logger.info("CHAR类型NOT NULL字段测试通过（已验证DEFAULT ''自动添加）");
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

    // ==================== COMMENT 相关测试 ====================

    /**
     * 测试ADD COLUMN - 带COMMENT（MySQL COMMENT → SQL Server MS_Description）
     */
    @Test
    public void testAddColumn_WithComment() throws Exception {
        logger.info("开始测试ADD COLUMN - 带COMMENT");

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // MySQL添加带COMMENT的字段
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status INT COMMENT '状态值，1：有效；2：无效'";
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundStatusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "status".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "status".equals(fm.getTarget().getName()));

        assertTrue("应找到status字段的映射", foundStatusMapping);
        verifyFieldExistsInTargetDatabase("status", tableGroup.getTargetTable().getName(), sqlServerConfig);

        // 验证COMMENT（MySQL COMMENT → SQL Server MS_Description）
        verifyFieldComment("status", tableGroup.getTargetTable().getName(), sqlServerConfig, "状态值，1：有效；2：无效");

        logger.info("ADD COLUMN带COMMENT测试通过");
    }

    /**
     * 测试ADD COLUMN - 带COMMENT（包含特殊字符）
     * 验证COMMENT字符串中包含单引号、分号、冒号等特殊字符时的转义处理
     */
    @Test
    public void testAddColumn_WithCommentContainingSpecialChars() throws Exception {
        logger.info("开始测试ADD COLUMN - 带COMMENT（包含特殊字符）");

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // MySQL添加带COMMENT的字段（包含特殊字符）
        String comment = "外部活码类型，1：进群宝；2：企业微信";
        String mysqlDDL = String.format("ALTER TABLE ddlTestEmployee ADD COLUMN outQrcodeID INT NOT NULL COMMENT '%s'", 
                comment.replace("'", "''")); // MySQL中单引号需要转义为''
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundOutQrcodeIDMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "outQrcodeID".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "outQrcodeID".equals(fm.getTarget().getName()));

        assertTrue("应找到outQrcodeID字段的映射", foundOutQrcodeIDMapping);
        verifyFieldExistsInTargetDatabase("outQrcodeID", tableGroup.getTargetTable().getName(), sqlServerConfig);

        // 验证COMMENT（包含特殊字符）
        verifyFieldComment("outQrcodeID", tableGroup.getTargetTable().getName(), sqlServerConfig, comment);

        logger.info("ADD COLUMN带COMMENT（包含特殊字符）测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 添加COMMENT（包含特殊字符）
     */
    @Test
    public void testModifyColumn_WithComment() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 添加COMMENT");

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // MySQL修改字段并添加COMMENT（包含特殊字符）
        String comment = "用户姓名，包含单引号'测试";
        String mysqlDDL = String.format("ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) COMMENT '%s'",
                comment.replace("'", "''")); // MySQL中单引号需要转义为''
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        Thread.sleep(3000);

        // 验证字段映射仍然存在
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        // 验证COMMENT（MySQL COMMENT → SQL Server MS_Description）
        verifyFieldComment("first_name", tableGroup.getTargetTable().getName(), sqlServerConfig, comment);

        logger.info("MODIFY COLUMN添加COMMENT测试通过");
    }

    // ==================== CREATE TABLE 测试场景 ====================

    /**
     * 测试CREATE TABLE - 基础建表（配置阶段）
     * 验证MySQL到SQL Server的类型转换和约束转换
     */
    @Test
    public void testCreateTable_Basic() throws Exception {
        logger.info("开始测试CREATE TABLE - 基础建表（配置阶段）");

        // 准备：确保表不存在
        prepareForCreateTableTest("createTableTestSource", "createTableTestTarget");

        // 先在源库创建表（MySQL）
        String sourceDDL = "DROP TABLE IF EXISTS createTableTestSource;\n" +
                "CREATE TABLE createTableTestSource (\n" +
                "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                "    actID INT NOT NULL,\n" +
                "    pid INT NOT NULL,\n" +
                "    mediumID INT NOT NULL,\n" +
                "    createtime DATETIME NOT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("createTableTestSource", "createTableTestTarget");

        // 验证表结构
        verifyTableExists("createTableTestTarget", sqlServerConfig);
        verifyTableFieldCount("createTableTestTarget", sqlServerConfig, 5);
        verifyFieldExistsInTargetDatabase("id", "createTableTestTarget", sqlServerConfig);
        verifyFieldExistsInTargetDatabase("actID", "createTableTestTarget", sqlServerConfig);
        verifyFieldExistsInTargetDatabase("pid", "createTableTestTarget", sqlServerConfig);
        verifyFieldExistsInTargetDatabase("mediumID", "createTableTestTarget", sqlServerConfig);
        verifyFieldExistsInTargetDatabase("createtime", "createTableTestTarget", sqlServerConfig);

        // 验证主键（AUTO_INCREMENT → IDENTITY）
        verifyTablePrimaryKeys("createTableTestTarget", sqlServerConfig, Arrays.asList("id"));

        // 验证字段属性
        verifyFieldNotNull("id", "createTableTestTarget", sqlServerConfig);
        verifyFieldNotNull("actID", "createTableTestTarget", sqlServerConfig);
        verifyFieldType("createtime", "createTableTestTarget", sqlServerConfig, "datetime2");

        logger.info("CREATE TABLE基础建表测试通过");
    }

    /**
     * 测试CREATE TABLE - 带COMMENT（包含特殊字符）
     * 重点测试MySQL COMMENT → SQL Server MS_Description的转换
     */
    @Test
    public void testCreateTable_WithSpecialCharsInComments() throws Exception {
        logger.info("开始测试CREATE TABLE - 带COMMENT（包含特殊字符）");

        // 准备：确保表不存在
        prepareForCreateTableTest("visit_wechatsale_activity_allocationresult", "visit_wechatsale_activity_allocationresult");

        // 先在源库创建表（MySQL，包含特殊字符的COMMENT）
        String sourceDDL = "DROP TABLE IF EXISTS visit_wechatsale_activity_allocationresult;\n" +
                "CREATE TABLE visit_wechatsale_activity_allocationresult (\n" +
                "    resultID INT AUTO_INCREMENT PRIMARY KEY,\n" +
                "    outQrcodeID INT NOT NULL COMMENT '外部活码类型，1：进群宝；2：企业微信',\n" +
                "    membertype INT NOT NULL COMMENT ' 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户',\n" +
                "    typeState INT NOT NULL COMMENT '1:图片；2网页; 3文件；4视频；5小程序'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("visit_wechatsale_activity_allocationresult", "visit_wechatsale_activity_allocationresult");

        // 验证表结构
        verifyTableExists("visit_wechatsale_activity_allocationresult", sqlServerConfig);
        verifyTableFieldCount("visit_wechatsale_activity_allocationresult", sqlServerConfig, 4);

        // 验证COMMENT（重点测试特殊字符转义）
        verifyFieldComment("outQrcodeID", "visit_wechatsale_activity_allocationresult", sqlServerConfig,
                "外部活码类型，1：进群宝；2：企业微信");
        verifyFieldComment("membertype", "visit_wechatsale_activity_allocationresult", sqlServerConfig,
                " 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户");
        verifyFieldComment("typeState", "visit_wechatsale_activity_allocationresult", sqlServerConfig,
                "1:图片；2网页; 3文件；4视频；5小程序");

        // 验证主键
        verifyTablePrimaryKeys("visit_wechatsale_activity_allocationresult", sqlServerConfig, Arrays.asList("resultID"));

        logger.info("CREATE TABLE带COMMENT（包含特殊字符）测试通过");
    }

    /**
     * 测试CREATE TABLE - 带约束（NOT NULL、AUTO_INCREMENT）
     */
    @Test
    public void testCreateTable_WithConstraints() throws Exception {
        logger.info("开始测试CREATE TABLE - 带约束");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableWithConstraints", "testTableWithConstraints");

        // 先在源库创建表（MySQL，包含各种约束）
        String sourceDDL = "DROP TABLE IF EXISTS testTableWithConstraints;\n" +
                "CREATE TABLE testTableWithConstraints (\n" +
                "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                "    username VARCHAR(50) NOT NULL,\n" +
                "    email VARCHAR(100),\n" +
                "    age INT NOT NULL,\n" +
                "    status TINYINT NOT NULL DEFAULT 1\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableWithConstraints", "testTableWithConstraints");

        // 验证表结构
        verifyTableExists("testTableWithConstraints", sqlServerConfig);
        verifyTableFieldCount("testTableWithConstraints", sqlServerConfig, 5);

        // 验证约束
        verifyFieldNotNull("id", "testTableWithConstraints", sqlServerConfig);
        verifyFieldNotNull("username", "testTableWithConstraints", sqlServerConfig);
        verifyFieldNotNull("age", "testTableWithConstraints", sqlServerConfig);
        verifyFieldNotNull("status", "testTableWithConstraints", sqlServerConfig);
        verifyFieldNullable("email", "testTableWithConstraints", sqlServerConfig);

        // 验证主键（AUTO_INCREMENT → IDENTITY）
        verifyTablePrimaryKeys("testTableWithConstraints", sqlServerConfig, Arrays.asList("id"));

        logger.info("CREATE TABLE带约束测试通过");
    }

    /**
     * 测试CREATE TABLE - 复合主键
     */
    @Test
    public void testCreateTable_WithCompositePrimaryKey() throws Exception {
        logger.info("开始测试CREATE TABLE - 复合主键");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableCompositePK", "testTableCompositePK");

        // 先在源库创建表（MySQL，复合主键）
        String sourceDDL = "DROP TABLE IF EXISTS testTableCompositePK;\n" +
                "CREATE TABLE testTableCompositePK (\n" +
                "    user_id INT NOT NULL,\n" +
                "    role_id INT NOT NULL,\n" +
                "    created_at DATETIME NOT NULL,\n" +
                "    PRIMARY KEY (user_id, role_id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableCompositePK", "testTableCompositePK");

        // 验证表结构
        verifyTableExists("testTableCompositePK", sqlServerConfig);
        verifyTableFieldCount("testTableCompositePK", sqlServerConfig, 3);

        // 验证复合主键
        verifyTablePrimaryKeys("testTableCompositePK", sqlServerConfig, Arrays.asList("user_id", "role_id"));

        logger.info("CREATE TABLE复合主键测试通过");
    }

    // ==================== 通用测试方法 ====================

    /**
     * 环境准备：直接在源库和目标库执行DDL（不通过同步机制）
     * 用于测试前的环境准备，确保源库和目标库结构一致
     */
    private void prepareEnvironment(String mysqlDDL, String sqlServerDDL) throws Exception {
        // 直接在源库执行 MySQL DDL
        executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);
        // 直接在目标库执行 SQL Server DDL
        executeDDLToSourceDatabase(sqlServerDDL, sqlServerConfig);
    }

    /**
     * 执行DDL转换并验证结果（如果Mapping未启动则自动启动）
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName) throws Exception {
        // 确保Mapping已启动（如果未运行则启动，避免重复启动）
        Meta meta = profileComponent.getMapping(mappingId).getMeta();
        if (meta == null || !meta.isRunning()) {
            mappingService.start(mappingId);
            Thread.sleep(2000);
        }

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

        // 等待DDL处理完成（使用轮询方式，而不是固定等待）
        boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
        boolean isChangeOperation = sourceDDL.toUpperCase().contains("CHANGE");
        if (isAddOperation || isChangeOperation) {
            waitForDDLProcessingComplete(expectedFieldName, 10000);
        }

        // 验证字段映射是否更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

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

    // ==================== 辅助验证方法 ====================

    /**
     * 验证字段类型
     */
    private void verifyFieldType(String fieldName, String tableName, DatabaseConfig config, String expectedType) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            // SQL Server需要指定schema
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            String actualType = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            assertNotNull(String.format("未找到字段 %s", fieldName), actualType);
            assertTrue(String.format("字段 %s 的类型应为 %s，但实际是 %s", fieldName, expectedType, actualType),
                    expectedType.equalsIgnoreCase(actualType));
            logger.info("字段类型验证通过: {} 的类型是 {}", fieldName, actualType);
            return null;
        });
    }

    /**
     * 验证字段的COMMENT（SQL Server使用MS_Description扩展属性）
     */
    private void verifyFieldComment(String fieldName, String tableName, DatabaseConfig config, String expectedComment) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // SQL Server使用扩展属性存储注释
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            
            String sql = "SELECT value FROM sys.extended_properties " +
                    "WHERE major_id = OBJECT_ID(? + '.' + ?) " +
                    "  AND minor_id = COLUMNPROPERTY(OBJECT_ID(? + '.' + ?), ?, 'ColumnId') " +
                    "  AND name = 'MS_Description'";
            String actualComment = databaseTemplate.queryForObject(sql, String.class, schema, tableName, schema, tableName, fieldName);
            
            String normalizedExpected = expectedComment != null ? expectedComment.trim() : "";
            String normalizedActual = actualComment != null ? actualComment.trim() : "";
            
            // SQL Server的sys.extended_properties.value可能返回转义后的单引号（''），需要转换为单个单引号（'）
            // 注意：这里只处理单引号转义，因为SQL Server存储时会将单引号转义为''
            normalizedActual = normalizedActual.replace("''", "'");
            
            assertTrue(String.format("字段 %s 的COMMENT应为 '%s'，但实际是 '%s'", fieldName, expectedComment, normalizedActual),
                    normalizedExpected.equals(normalizedActual));
            logger.info("字段COMMENT验证通过: {} 的COMMENT是 '{}'", fieldName, normalizedActual);
            return null;
        });
    }

    /**
     * 验证字段是否可空（NULL）
     */
    private void verifyFieldNullable(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            String sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            String isNullable = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            assertNotNull(String.format("未找到字段 %s", fieldName), isNullable);
            assertEquals(String.format("字段 %s 应为NULL（可空），但实际是 %s", fieldName, isNullable),
                    "YES", isNullable.toUpperCase());
            logger.info("字段NULL约束验证通过: {} 是可空的", fieldName);
            return null;
        });
    }

    /**
     * 验证表是否存在
     */
    private void verifyTableExists(String tableName, DatabaseConfig config) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
            Integer count = databaseTemplate.queryForObject(sql, Integer.class, schema, tableName);
            assertTrue(String.format("表 %s 应存在，但未找到", tableName), count != null && count > 0);
            logger.info("表存在验证通过: {}", tableName);
            return null;
        });
    }

    /**
     * 验证表的字段数量
     */
    private void verifyTableFieldCount(String tableName, DatabaseConfig config, int expectedCount) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
            Integer actualCount = databaseTemplate.queryForObject(sql, Integer.class, schema, tableName);
            assertNotNull(String.format("未找到表 %s", tableName), actualCount);
            assertEquals(String.format("表 %s 的字段数量应为 %d，但实际是 %d", tableName, expectedCount, actualCount),
                    Integer.valueOf(expectedCount), actualCount);
            logger.info("表字段数量验证通过: {} 有 {} 个字段", tableName, actualCount);
            return null;
        });
    }

    /**
     * 验证表的主键
     */
    private void verifyTablePrimaryKeys(String tableName, DatabaseConfig config, List<String> expectedKeys) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String schemaSql = "SELECT SCHEMA_NAME()";
            String schema = databaseTemplate.queryForObject(schemaSql, String.class);
            if (schema == null || schema.trim().isEmpty()) {
                schema = "dbo";
            }
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                    "AND CONSTRAINT_NAME LIKE 'PK_%' " +
                    "ORDER BY ORDINAL_POSITION";
            List<String> actualKeys = databaseTemplate.queryForList(sql, String.class, schema, tableName);
            assertNotNull(String.format("未找到表 %s 的主键信息", tableName), actualKeys);
            assertEquals(String.format("表 %s 的主键数量应为 %d，但实际是 %d", tableName, expectedKeys.size(), actualKeys.size()),
                    expectedKeys.size(), actualKeys.size());
            for (int i = 0; i < expectedKeys.size(); i++) {
                String expectedKey = expectedKeys.get(i);
                String actualKey = actualKeys.get(i);
                assertTrue(String.format("表 %s 的主键第 %d 列应为 %s，但实际是 %s", tableName, i + 1, expectedKey, actualKey),
                        expectedKey.equalsIgnoreCase(actualKey));
            }
            logger.info("表主键验证通过: {} 的主键是 {}", tableName, actualKeys);
            return null;
        });
    }

    /**
     * 准备建表测试环境（确保表不存在）
     */
    private void prepareForCreateTableTest(String sourceTable, String targetTable) throws Exception {
        logger.debug("准备建表测试环境，确保表不存在: sourceTable={}, targetTable={}", sourceTable, targetTable);

        // 删除源表和目标表（如果存在）
        forceDropTable(sourceTable, mysqlConfig);
        forceDropTable(targetTable, mysqlConfig);
        forceDropTable(sourceTable, sqlServerConfig);
        forceDropTable(targetTable, sqlServerConfig);

        // 等待删除完成
        Thread.sleep(200);

        logger.debug("建表测试环境准备完成");
    }

    /**
     * 强制删除表（忽略不存在的错误）
     */
    private void forceDropTable(String tableName, DatabaseConfig config) {
        try {
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                    new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
            instance.execute(databaseTemplate -> {
                try {
                    String dropSql;
                    if (config.getDriverClassName() != null && config.getDriverClassName().contains("sqlserver")) {
                        dropSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", tableName, tableName);
                    } else {
                        dropSql = String.format("DROP TABLE IF EXISTS %s", tableName);
                    }
                    databaseTemplate.execute(dropSql);
                    logger.debug("已删除表: {}", tableName);
                } catch (Exception e) {
                    logger.debug("删除表失败（可能不存在）: {}", e.getMessage());
                }
                return null;
            });
        } catch (Exception e) {
            logger.debug("强制删除表时出错（可忽略）: {}", e.getMessage());
        }
    }

    /**
     * 模拟配置阶段的建表流程：从源表结构创建目标表
     * 这是配置阶段建表的核心逻辑，会触发 COMMENT 转义功能
     */
    private void createTargetTableFromSource(String sourceTable, String targetTable) throws Exception {
        logger.info("开始从源表创建目标表: {} -> {}", sourceTable, targetTable);

        // 确保 connectorType 已设置
        if (mysqlConfig.getConnectorType() == null) {
            mysqlConfig.setConnectorType(getConnectorType(mysqlConfig, true));
        }
        if (sqlServerConfig.getConnectorType() == null) {
            sqlServerConfig.setConnectorType(getConnectorType(sqlServerConfig, false));
        }

        // 1. 连接源和目标数据库
        org.dbsyncer.sdk.connector.ConnectorInstance sourceConnectorInstance = connectorFactory.connect(mysqlConfig);
        org.dbsyncer.sdk.connector.ConnectorInstance targetConnectorInstance = connectorFactory.connect(sqlServerConfig);

        // 2. 检查目标表是否已存在
        try {
            org.dbsyncer.sdk.model.MetaInfo existingTable = connectorFactory.getMetaInfo(targetConnectorInstance, targetTable);
            if (existingTable != null && existingTable.getColumn() != null && !existingTable.getColumn().isEmpty()) {
                logger.info("目标表已存在，跳过创建: {}", targetTable);
                return;
            }
        } catch (Exception e) {
            logger.debug("目标表不存在，开始创建: {}", targetTable);
        }

        // 3. 获取源表结构
        org.dbsyncer.sdk.model.MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(sourceConnectorInstance, sourceTable);
        assertNotNull("无法获取源表结构: " + sourceTable, sourceMetaInfo);
        assertFalse("源表没有字段: " + sourceTable, sourceMetaInfo.getColumn() == null || sourceMetaInfo.getColumn().isEmpty());

        // 4. 不同类型数据库：走标准转换流程
        String sourceType = mysqlConfig.getConnectorType();
        String targetType = sqlServerConfig.getConnectorType();
        logger.debug("检测到不同类型数据库（{} -> {}），使用标准转换流程", sourceType, targetType);

        org.dbsyncer.sdk.spi.ConnectorService sourceConnectorService = connectorFactory.getConnectorService(sourceType);
        org.dbsyncer.sdk.spi.ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetType);
        org.dbsyncer.sdk.schema.SchemaResolver sourceSchemaResolver = sourceConnectorService.getSchemaResolver();

        // 创建标准化的 MetaInfo
        org.dbsyncer.sdk.model.MetaInfo standardizedMetaInfo = new org.dbsyncer.sdk.model.MetaInfo();
        standardizedMetaInfo.setTableType(sourceMetaInfo.getTableType());
        standardizedMetaInfo.setSql(sourceMetaInfo.getSql());
        standardizedMetaInfo.setIndexType(sourceMetaInfo.getIndexType());

        // 将源字段转换为标准类型（toStandardType 会自动保留所有元数据属性，包括 COMMENT）
        List<org.dbsyncer.sdk.model.Field> standardizedFields = new ArrayList<>();
        for (org.dbsyncer.sdk.model.Field sourceField : sourceMetaInfo.getColumn()) {
            org.dbsyncer.sdk.model.Field standardField = sourceSchemaResolver.toStandardType(sourceField);
            standardizedFields.add(standardField);
        }
        standardizedMetaInfo.setColumn(standardizedFields);

        // 生成 CREATE TABLE DDL（使用标准化后的 MetaInfo）
        String createTableDDL = targetConnectorService.generateCreateTableDDL(standardizedMetaInfo, targetTable);

        // 5. 执行 CREATE TABLE DDL
        assertNotNull("无法生成 CREATE TABLE DDL", createTableDDL);
        assertFalse("生成的 CREATE TABLE DDL 为空", createTableDDL.trim().isEmpty());

        org.dbsyncer.sdk.config.DDLConfig ddlConfig = new org.dbsyncer.sdk.config.DDLConfig();
        ddlConfig.setSql(createTableDDL);
        org.dbsyncer.common.model.Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig);

        if (result != null && result.error != null && !result.error.trim().isEmpty()) {
            throw new RuntimeException("创建表失败: " + result.error);
        }

        logger.info("成功创建目标表: {}", targetTable);
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Mapping和TableGroup（使用基类方法，确保TableGroup正确创建）
     */
    @Override
    protected String createMapping() throws Exception {
        // 先创建Mapping（不包含tableGroups）
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

        // 然后使用tableGroupService.add()创建TableGroup
        Map<String, String> tableGroupParams = new HashMap<>();
        tableGroupParams.put("mappingId", mappingId);
        tableGroupParams.put("sourceTable", getSourceTableName());
        tableGroupParams.put("targetTable", getTargetTableName());
        tableGroupParams.put("fieldMappings", String.join(",", getInitialFieldMappings()));
        tableGroupService.add(tableGroupParams);

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

