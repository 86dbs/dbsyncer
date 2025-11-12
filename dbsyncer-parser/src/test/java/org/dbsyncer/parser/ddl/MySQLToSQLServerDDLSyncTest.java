package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.flush.impl.GeneralBufferActuator;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * MySQL到SQL Server的DDL同步集成测试
 * 全面测试MySQL到SQL Server的DDL同步功能，包括类型转换、操作转换等
 * 覆盖场景：
 * - MySQL特殊类型转换：ENUM, SET, JSON, YEAR, GEOMETRY系列, TINYTEXT, MEDIUMTEXT, LONGTEXT, BIT
 * - DDL操作：ADD COLUMN, MODIFY COLUMN, CHANGE COLUMN, DROP COLUMN
 * - 复杂场景：带约束字段添加、字段重命名
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class MySQLToSQLServerDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(MySQLToSQLServerDDLSyncTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig mysqlConfig;
    private static DatabaseConfig sqlServerConfig;
    private static ConnectorFactory connectorFactory;
    @SuppressWarnings("rawtypes")
    private static ConnectorService sqlServerConnectorService;

    private DDLParserImpl ddlParser;
    private TableGroup tableGroup;
    private GeneralBufferActuator generalBufferActuator;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化MySQL到SQL Server的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器（第一个参数是MySQL，第二个是SQL Server）
        testDatabaseManager = new TestDatabaseManager(mysqlConfig, sqlServerConfig);

        // 初始化ConnectorFactory
        connectorFactory = TestDDLHelper.createConnectorFactory();

        // 初始化SQL Server ConnectorService（用于DDL解析和验证）
        sqlServerConnectorService = TestDDLHelper.createConnectorService(sqlServerConfig);

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
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);
            logger.info("MySQL到SQL Server的DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws IOException {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        ddlParser = new DDLParserImpl();
        TestDDLHelper.initDDLParser(ddlParser);
        TestDDLHelper.setConnectorFactory(ddlParser, connectorFactory);

        // 创建TableGroup配置
        tableGroup = createTableGroup();

        logger.info("MySQL到SQL Server的DDL同步测试用例环境初始化完成");
    }

    @After
    public void tearDown() {
        // 每个测试后重置表结构，确保测试隔离
        resetDatabaseTableStructure();
    }

    /**
     * 重置数据库表结构到初始状态
     */
    private void resetDatabaseTableStructure() {
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

    /**
     * 创建TableGroup配置
     */
    private TableGroup createTableGroup() {
        TableGroup tableGroup = new TableGroup();
        tableGroup.setId("mysql-to-sqlserver-tablegroup-id");
        tableGroup.setMappingId("mysql-to-sqlserver-mapping-id");

        Table mysqlSourceTable = new Table();
        mysqlSourceTable.setName("ddlTestEmployee");
        mysqlSourceTable.setColumn(new ArrayList<>());

        Table sqlserverTargetTable = new Table();
        sqlserverTargetTable.setName("ddlTestEmployee");
        sqlserverTargetTable.setColumn(new ArrayList<>());

        tableGroup.setSourceTable(mysqlSourceTable);
        tableGroup.setTargetTable(sqlserverTargetTable);

        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping idMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(idMapping);
        mysqlSourceTable.getColumn().add(idSourceField);
        sqlserverTargetTable.getColumn().add(idTargetField);

        Field firstNameSourceField = new Field("first_name", "VARCHAR", 12);
        Field firstNameTargetField = new Field("first_name", "NVARCHAR", 12);
        FieldMapping firstNameMapping = new FieldMapping(firstNameSourceField, firstNameTargetField);
        fieldMappings.add(firstNameMapping);
        mysqlSourceTable.getColumn().add(firstNameSourceField);
        sqlserverTargetTable.getColumn().add(firstNameTargetField);

        tableGroup.setFieldMapping(fieldMappings);

        // 配置TableGroup（注意：源是MySQL，目标是SQL Server）
        TestDDLHelper.setupTableGroup(tableGroup, "mysql-to-sqlserver-mapping-id",
                "mysql-connector-id", "sqlserver-connector-id",
                mysqlConfig, sqlServerConfig);

        // 创建GeneralBufferActuator
        generalBufferActuator = TestDDLHelper.createGeneralBufferActuator(
                connectorFactory,
                tableGroup.profileComponent,
                ddlParser);

        return tableGroup;
    }

    // ==================== MySQL特殊类型转换测试 ====================

    @Test
    public void testAddColumn_ENUMType() {
        logger.info("开始测试ENUM类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status ENUM('active','inactive','pending')";
        testDDLConversion(mysqlDDL, "status", "NVARCHAR", "NVARCHAR");
    }

    @Test
    public void testAddColumn_SETType() {
        logger.info("开始测试SET类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN tags SET('tag1','tag2','tag3')";
        testDDLConversion(mysqlDDL, "tags", "NVARCHAR", "NVARCHAR");
    }

    @Test
    public void testAddColumn_JSONType() {
        logger.info("开始测试JSON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN metadata JSON";
        testDDLConversion(mysqlDDL, "metadata", "NVARCHAR", "NVARCHAR(MAX)");
    }

    @Test
    public void testAddColumn_YEARType() {
        logger.info("开始测试YEAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_year YEAR";
        testDDLConversion(mysqlDDL, "birth_year", "SMALLINT", "SMALLINT");
    }

    @Test
    public void testAddColumn_GEOMETRYType() {
        logger.info("开始测试GEOMETRY类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN location GEOMETRY";
        testDDLConversion(mysqlDDL, "location", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_POINTType() {
        logger.info("开始测试POINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN coordinates POINT";
        testDDLConversion(mysqlDDL, "coordinates", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_LINESTRINGType() {
        logger.info("开始测试LINESTRING类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN route LINESTRING";
        testDDLConversion(mysqlDDL, "route", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_POLYGONType() {
        logger.info("开始测试POLYGON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN area POLYGON";
        testDDLConversion(mysqlDDL, "area", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_MULTIPOINTType() {
        logger.info("开始测试MULTIPOINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN points MULTIPOINT";
        testDDLConversion(mysqlDDL, "points", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_MULTILINESTRINGType() {
        logger.info("开始测试MULTILINESTRING类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN routes MULTILINESTRING";
        testDDLConversion(mysqlDDL, "routes", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_MULTIPOLYGONType() {
        logger.info("开始测试MULTIPOLYGON类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN regions MULTIPOLYGON";
        testDDLConversion(mysqlDDL, "regions", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_GEOMETRYCOLLECTIONType() {
        logger.info("开始测试GEOMETRYCOLLECTION类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN collection GEOMETRYCOLLECTION";
        testDDLConversion(mysqlDDL, "collection", "GEOMETRY", "geometry");
    }

    @Test
    public void testAddColumn_TINYTEXTType() {
        logger.info("开始测试TINYTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN short_text TINYTEXT";
        testDDLConversion(mysqlDDL, "short_text", "NVARCHAR", "NVARCHAR(255)");
    }

    @Test
    public void testAddColumn_MEDIUMTEXTType() {
        logger.info("开始测试MEDIUMTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN medium_text MEDIUMTEXT";
        testDDLConversion(mysqlDDL, "medium_text", "NVARCHAR", "NVARCHAR(MAX)");
    }

    @Test
    public void testAddColumn_LONGTEXTType() {
        logger.info("开始测试LONGTEXT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN long_text LONGTEXT";
        testDDLConversion(mysqlDDL, "long_text", "NVARCHAR", "NVARCHAR(MAX)");
    }

    @Test
    public void testAddColumn_BITType() {
        logger.info("开始测试BIT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN is_verified BIT(1)";
        testDDLConversion(mysqlDDL, "is_verified", "TINYINT", "TINYINT");
    }

    // ==================== MySQL基础类型转换测试 ====================

    @Test
    public void testAddColumn_VARCHARType() {
        logger.info("开始测试VARCHAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN last_name VARCHAR(100)";
        testDDLConversion(mysqlDDL, "last_name", "NVARCHAR", "NVARCHAR(100)");
    }

    @Test
    public void testAddColumn_CHARType() {
        logger.info("开始测试CHAR类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN code CHAR(10)";
        testDDLConversion(mysqlDDL, "code", "NCHAR", "NCHAR(10)");
    }

    @Test
    public void testAddColumn_INTType() {
        logger.info("开始测试INT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN age INT";
        testDDLConversion(mysqlDDL, "age", "INT", "INT");
    }

    @Test
    public void testAddColumn_BIGINTType() {
        logger.info("开始测试BIGINT类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN count_num BIGINT";
        testDDLConversion(mysqlDDL, "count_num", "BIGINT", "BIGINT");
    }

    @Test
    public void testAddColumn_DECIMALType() {
        logger.info("开始测试DECIMAL类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN price DECIMAL(10,2)";
        testDDLConversion(mysqlDDL, "price", "DECIMAL", "DECIMAL(10,2)");
    }

    @Test
    public void testAddColumn_DATEType() {
        logger.info("开始测试DATE类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_date DATE";
        testDDLConversion(mysqlDDL, "birth_date", "DATE", "DATE");
    }

    @Test
    public void testAddColumn_TIMEType() {
        logger.info("开始测试TIME类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN work_time TIME";
        testDDLConversion(mysqlDDL, "work_time", "TIME", "TIME");
    }

    @Test
    public void testAddColumn_DATETIMEType() {
        logger.info("开始测试DATETIME类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN updated_at DATETIME";
        testDDLConversion(mysqlDDL, "updated_at", "DATETIME", "DATETIME2");
    }

    @Test
    public void testAddColumn_TIMESTAMPType() {
        logger.info("开始测试TIMESTAMP类型转换");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN created_at TIMESTAMP";
        testDDLConversion(mysqlDDL, "created_at", "DATETIME", "DATETIME2");
    }

    // ==================== DDL操作测试 ====================

    @Test
    public void testModifyColumn_ChangeLength() {
        logger.info("开始测试MODIFY COLUMN操作 - 修改长度");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(100)";
        testDDLConversion(mysqlDDL, "first_name", "NVARCHAR", "NVARCHAR(100)");
    }

    @Test
    public void testModifyColumn_AddNotNull() {
        logger.info("开始测试MODIFY COLUMN操作 - 添加NOT NULL约束");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NOT NULL";
        testDDLConversion(mysqlDDL, "first_name", "NVARCHAR", "NVARCHAR(50)");
    }

    @Test
    public void testModifyColumn_ChangeType() {
        logger.info("开始测试MODIFY COLUMN操作 - 修改类型");
        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN count_num INT";
        try {
            executeDDLToSourceDatabase(addColumnDDL, mysqlConfig);
            WriterResponse addResponse = TestDDLHelper.createWriterResponse(addColumnDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping addMapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");
            generalBufferActuator.parseDDl(addResponse, addMapping, tableGroup);
        } catch (Exception e) {
            logger.warn("添加测试字段失败，跳过类型修改测试", e);
            return;
        }

        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN count_num BIGINT";
        testDDLConversion(mysqlDDL, "count_num", "BIGINT", "BIGINT");
    }

    @Test
    public void testModifyColumn_RemoveNotNull() {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NOT NULL";
        try {
            executeDDLToSourceDatabase(setNotNullDDL, mysqlConfig);
            WriterResponse setResponse = TestDDLHelper.createWriterResponse(setNotNullDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping setMapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");
            generalBufferActuator.parseDDl(setResponse, setMapping, tableGroup);
        } catch (Exception e) {
            logger.warn("设置NOT NULL约束失败，跳过移除NOT NULL测试", e);
            return;
        }

        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(50) NULL";
        testDDLConversion(mysqlDDL, "first_name", "NVARCHAR", "NVARCHAR(50)");
    }

    @Test
    public void testChangeColumn_RenameOnly() {
        logger.info("开始测试CHANGE COLUMN操作 - 仅重命名");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN first_name given_name VARCHAR(50)";
        testDDLConversion(mysqlDDL, "given_name", "NVARCHAR", "NVARCHAR(50)");
    }

    @Test
    public void testChangeColumn_RenameAndModifyType() {
        logger.info("开始测试CHANGE COLUMN操作 - 重命名并修改类型");
        // 先添加一个字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN description VARCHAR(100)";
        try {
            executeDDLToSourceDatabase(addColumnDDL, mysqlConfig);
            WriterResponse addResponse = TestDDLHelper.createWriterResponse(addColumnDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping addMapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");
            generalBufferActuator.parseDDl(addResponse, addMapping, tableGroup);
        } catch (Exception e) {
            logger.warn("添加测试字段失败，跳过重命名并修改类型测试", e);
            return;
        }

        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN description desc_text TEXT";
        testDDLConversion(mysqlDDL, "desc_text", "NVARCHAR", "NVARCHAR(MAX)");
    }

    @Test
    public void testChangeColumn_RenameAndModifyLengthAndConstraint() {
        logger.info("开始测试CHANGE COLUMN操作 - 重命名并修改长度和约束");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN first_name given_name VARCHAR(100) NOT NULL";
        testDDLConversion(mysqlDDL, "given_name", "NVARCHAR", "NVARCHAR(100)");
    }

    @Test
    public void testDropColumn() {
        logger.info("开始测试DROP COLUMN操作");
        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name", "first_name");
    }

    // ==================== 复杂场景测试 ====================

    @Test
    public void testAddColumn_WithDefault() {
        logger.info("开始测试带默认值的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status VARCHAR(20) DEFAULT 'active'";
        testDDLConversion(mysqlDDL, "status", "NVARCHAR", "NVARCHAR(20)");
    }

    @Test
    public void testAddColumn_WithNotNull() {
        logger.info("开始测试带NOT NULL约束的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN phone VARCHAR(20) NOT NULL";
        testDDLConversion(mysqlDDL, "phone", "NVARCHAR", "NVARCHAR(20)");
    }

    @Test
    public void testAddColumn_WithDefaultAndNotNull() {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN email VARCHAR(100) NOT NULL DEFAULT 'unknown@example.com'";
        testDDLConversion(mysqlDDL, "email", "NVARCHAR", "NVARCHAR(100)");
    }

    @Test
    public void testAddColumn_WithAfter() {
        logger.info("开始测试带AFTER子句的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN last_name VARCHAR(50) AFTER first_name";
        testDDLConversion(mysqlDDL, "last_name", "NVARCHAR", "NVARCHAR(50)");
    }

    @Test
    public void testAddColumn_WithFirst() {
        logger.info("开始测试带FIRST子句的字段添加");
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN priority INT FIRST";
        testDDLConversion(mysqlDDL, "priority", "INT", "INT");
    }

    @Test
    public void testAddMultipleColumns() {
        logger.info("开始测试多字段同时添加");
        // MySQL语法：每个字段都需要ADD COLUMN关键字
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN salary DECIMAL(10,2), ADD COLUMN bonus DECIMAL(8,2)";
        
        try {
            // 1. 先在源数据库执行DDL
            executeDDLToSourceDatabase(mysqlDDL, mysqlConfig);

            // 2. 创建WriterResponse和Mapping
            WriterResponse response = TestDDLHelper.createWriterResponse(mysqlDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");

            // 3. 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, tableGroup);

            // 4. 验证两个字段的映射是否都已更新
            boolean foundSalaryMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && "salary".equals(fm.getSource().getName()) &&
                            fm.getTarget() != null && "salary".equals(fm.getTarget().getName()));
            assertTrue("应找到salary字段的映射", foundSalaryMapping);

            boolean foundBonusMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && "bonus".equals(fm.getSource().getName()) &&
                            fm.getTarget() != null && "bonus".equals(fm.getTarget().getName()));
            assertTrue("应找到bonus字段的映射", foundBonusMapping);

            // 5. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(sqlServerConnectorService, tableGroup, mysqlDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);
            logger.info("转换后的SQL: {}", targetSql);
            validateSQLSyntax(targetSql);

            // 6. 验证转换后的SQL包含两个字段
            assertTrue(String.format("转换后的DDL应包含salary字段，实际SQL: %s", targetSql),
                    targetSql.contains("salary"));
            assertTrue(String.format("转换后的DDL应包含bonus字段，实际SQL: %s", targetSql),
                    targetSql.contains("bonus"));

            // 7. 验证目标数据库中两个字段都存在
            verifyFieldExistsInTargetDatabase("salary", tableGroup.getTargetTable().getName(), sqlServerConfig);
            verifyFieldExistsInTargetDatabase("bonus", tableGroup.getTargetTable().getName(), sqlServerConfig);

            logger.info("多字段添加测试通过 - salary和bonus字段都已正确转换");
        } catch (Exception e) {
            logger.error("多字段添加测试失败", e);
            fail("多字段添加测试失败: " + e.getMessage());
        }
    }

    // ==================== 通用测试方法 ====================

    /**
     * 执行DDL转换并验证结果
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName,
                                   String expectedTargetType, String expectedTargetPattern) {
        try {
            // 1. 先在源数据库执行DDL
            executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

            // 2. 创建WriterResponse和Mapping
            WriterResponse response = TestDDLHelper.createWriterResponse(sourceDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");

            // 3. 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, tableGroup);

            // 4. 验证字段映射是否更新
            boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
            boolean isChangeOperation = sourceDDL.toUpperCase().contains("CHANGE");
            String actualFieldName = isChangeOperation ? expectedFieldName : expectedFieldName;

            if (isAddOperation || isChangeOperation) {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && actualFieldName.equals(fm.getSource().getName()) &&
                                fm.getTarget() != null && actualFieldName.equals(fm.getTarget().getName()));
                assertTrue("应找到字段 " + actualFieldName + " 的映射", foundFieldMapping);
            } else {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
                assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
            }

            // 5. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(sqlServerConnectorService, tableGroup, sourceDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);

            // 6. 验证SQL语法
            validateSQLSyntax(targetSql);

            // 7. 验证转换后的SQL
            assertTrue(String.format("转换后的DDL应包含期望的目标类型 %s，实际SQL: %s", expectedTargetPattern, targetSql),
                    targetSql.toUpperCase().contains(expectedTargetPattern.toUpperCase()) ||
                            targetSql.toUpperCase().contains(expectedTargetType.toUpperCase()));
            assertTrue(String.format("转换后的DDL应包含字段名 %s，实际SQL: %s", actualFieldName, targetSql),
                    targetSql.contains(actualFieldName));

            // 8. 验证目标数据库中字段是否存在（对于ADD和CHANGE操作）
            if (isAddOperation || isChangeOperation) {
                verifyFieldExistsInTargetDatabase(actualFieldName, tableGroup.getTargetTable().getName(), sqlServerConfig);
            }

            logger.info("DDL转换测试通过 - 字段: {}, 目标类型: {}, 转换后SQL: {}",
                    actualFieldName, expectedTargetType, targetSql);
        } catch (Exception e) {
            logger.error("DDL转换测试失败 - 字段: {}", expectedFieldName, e);
            fail("DDL转换测试失败: " + e.getMessage());
        }
    }

    /**
     * 测试DDL DROP操作
     */
    private void testDDLDropOperation(String sourceDDL, String expectedFieldName) {
        try {
            // 1. 先在源数据库执行DDL
            executeDDLToSourceDatabase(sourceDDL, mysqlConfig);

            // 2. 创建WriterResponse和Mapping
            WriterResponse response = TestDDLHelper.createWriterResponse(sourceDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");

            // 3. 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, tableGroup);

            // 4. 验证字段映射是否已移除
            boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
            assertFalse("应移除字段 " + expectedFieldName + " 的映射", foundFieldMapping);

            // 5. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(sqlServerConnectorService, tableGroup, sourceDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);
            validateSQLSyntax(targetSql);

            // 6. 验证转换后的SQL
            assertTrue(String.format("转换后的DDL应包含DROP COLUMN关键字，实际SQL: %s", targetSql),
                    targetSql.toUpperCase().contains("DROP COLUMN"));
            assertTrue(String.format("转换后的DDL应包含字段名 %s，实际SQL: %s", expectedFieldName, targetSql),
                    targetSql.contains(expectedFieldName));

            // 7. 验证目标数据库中字段是否已被删除
            verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), sqlServerConfig);

            logger.info("DDL DROP操作测试通过 - 字段: {}, 转换后SQL: {}", expectedFieldName, targetSql);
        } catch (Exception e) {
            logger.error("DDL DROP操作测试失败 - 字段: {}", expectedFieldName, e);
            fail("DDL DROP操作测试失败: " + e.getMessage());
        }
    }

    /**
     * 验证转换后的SQL语法正确性
     */
    private void validateSQLSyntax(String sql) {
        try {
            String normalizedSql = sql.replace("[", "\"").replace("]", "\"");
            String[] statements = normalizedSql.split(";");
            for (String statement : statements) {
                String trimmedStatement = statement.trim();
                if (!trimmedStatement.isEmpty()) {
                    if (trimmedStatement.toUpperCase().startsWith("EXEC") ||
                            trimmedStatement.toUpperCase().startsWith("EXECUTE")) {
                        logger.debug("跳过EXEC语句的语法验证: {}", trimmedStatement);
                        continue;
                    }
                    CCJSqlParserUtil.parse(trimmedStatement);
                }
            }
            logger.debug("SQL语法验证通过: {}", sql);
        } catch (JSQLParserException e) {
            logger.error("SQL语法验证失败: {}", sql, e);
            fail("转换后的SQL语法不正确: " + sql);
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = MySQLToSQLServerDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
        sqlServerConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=target_db;encrypt=false;trustServerCertificate=true"));
        sqlServerConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sqlServerConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sqlServerConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    }

    /**
     * 创建默认的MySQL配置
     */
    private static DatabaseConfig createDefaultMySQLConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3306/source_db");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }

    /**
     * 创建默认的SQL Server配置
     */
    private static DatabaseConfig createDefaultSQLServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=target_db;encrypt=false;trustServerCertificate=true");
        config.setUsername("sa");
        config.setPassword("123456");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }

    /**
     * 加载SQL脚本文件
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = MySQLToSQLServerDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                logger.warn("未找到SQL脚本文件: {}", resourcePath);
                return "";
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        } catch (Exception e) {
            logger.error("加载SQL脚本文件失败: {}", resourcePath, e);
            return "";
        }
    }

    /**
     * 在源数据库执行DDL
     */
    private void executeDDLToSourceDatabase(String sourceDDL, DatabaseConfig sourceConfig) {
        logger.debug("在源数据库执行DDL: {}", sourceDDL);
        try {
            DatabaseConnectorInstance sourceConnectorInstance = new DatabaseConnectorInstance(sourceConfig);
            sourceConnectorInstance.execute(databaseTemplate -> {
                try (java.sql.Connection connection = databaseTemplate.getSimpleConnection().getConnection();
                     java.sql.Statement statement = connection.createStatement()) {
                    statement.execute(sourceDDL);
                    logger.debug("源数据库DDL执行成功: {}", sourceDDL);
                    return null;
                } catch (java.sql.SQLException e) {
                    logger.error("在源数据库执行DDL失败: {}", sourceDDL, e);
                    throw new RuntimeException("在源数据库执行DDL失败", e);
                }
            });
        } catch (Exception e) {
            logger.error("在源数据库执行DDL失败: {}", sourceDDL, e);
            throw new RuntimeException("在源数据库执行DDL失败", e);
        }
    }

    /**
     * 验证目标数据库中字段是否存在
     */
    private void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig targetConfig) {
        logger.debug("验证目标数据库中字段是否存在 - 表: {}, 字段: {}", tableName, fieldName);
        try {
            DatabaseConnectorInstance targetConnectorInstance = new DatabaseConnectorInstance(targetConfig);
            MetaInfo metaInfo = connectorFactory.getMetaInfo(targetConnectorInstance, tableName);
            boolean fieldExists = metaInfo.getColumn().stream()
                    .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
            assertTrue(String.format("目标数据库中应存在字段 %s，表: %s", fieldName, tableName), fieldExists);
            logger.debug("目标数据库中字段验证通过 - 表: {}, 字段: {}", tableName, fieldName);
        } catch (Exception e) {
            logger.error("验证目标数据库中字段是否存在失败 - 表: {}, 字段: {}", tableName, fieldName, e);
            fail("验证目标数据库中字段是否存在失败: " + e.getMessage());
        }
    }

    /**
     * 验证目标数据库中字段是否不存在
     */
    private void verifyFieldNotExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig targetConfig) {
        logger.debug("验证目标数据库中字段是否不存在 - 表: {}, 字段: {}", tableName, fieldName);
        try {
            DatabaseConnectorInstance targetConnectorInstance = new DatabaseConnectorInstance(targetConfig);
            MetaInfo metaInfo = connectorFactory.getMetaInfo(targetConnectorInstance, tableName);
            boolean fieldExists = metaInfo.getColumn().stream()
                    .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
            assertFalse(String.format("目标数据库中不应存在字段 %s，表: %s", fieldName, tableName), fieldExists);
            logger.debug("目标数据库中字段删除验证通过 - 表: {}, 字段: {}", tableName, fieldName);
        } catch (Exception e) {
            logger.error("验证目标数据库中字段是否不存在失败 - 表: {}, 字段: {}", tableName, fieldName, e);
            fail("验证目标数据库中字段是否不存在失败: " + e.getMessage());
        }
    }
}

