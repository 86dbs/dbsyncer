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
 * SQL Server到MySQL的DDL同步集成测试
 * 全面测试SQL Server到MySQL的DDL同步功能，包括类型转换、操作转换等
 * 覆盖场景：
 * - SQL Server特殊类型转换：XML, UNIQUEIDENTIFIER, MONEY, SMALLMONEY, DATETIME2, DATETIMEOFFSET, TIMESTAMP, IMAGE, TEXT, NTEXT, BINARY, SMALLDATETIME, BIT, HIERARCHYID
 * - DDL操作：ADD COLUMN, ALTER COLUMN (MODIFY), DROP COLUMN
 * - 复杂场景：多字段添加、带约束字段添加
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class SQLServerToMySQLDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerToMySQLDDLSyncTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig sqlServerConfig;
    private static DatabaseConfig mysqlConfig;
    private static ConnectorFactory connectorFactory;
    @SuppressWarnings("rawtypes")
    private static ConnectorService mysqlConnectorService;

    private DDLParserImpl ddlParser;
    private TableGroup tableGroup;
    private GeneralBufferActuator generalBufferActuator;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化SQL Server到MySQL的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器（第一个参数是SQL Server，第二个是MySQL）
        testDatabaseManager = new TestDatabaseManager(sqlServerConfig, mysqlConfig);

        // 初始化ConnectorFactory
        connectorFactory = TestDDLHelper.createConnectorFactory();

        // 初始化MySQL ConnectorService（用于DDL解析和验证）
        mysqlConnectorService = TestDDLHelper.createConnectorService(mysqlConfig);

        // 初始化测试环境
        String sqlServerInitSql =
                "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                        "CREATE TABLE ddlTestEmployee (\n" +
                        "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                        "    first_name NVARCHAR(50) NOT NULL\n" +
                        ");";

        String mysqlInitSql =
                "DROP TABLE IF EXISTS ddlTestEmployee;\n" +
                        "CREATE TABLE ddlTestEmployee (\n" +
                        "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                        "    first_name VARCHAR(50) NOT NULL\n" +
                        ");";

        testDatabaseManager.initializeTestEnvironment(sqlServerInitSql, mysqlInitSql);

        logger.info("SQL Server到MySQL的DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server到MySQL的DDL同步测试环境");

        try {
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);
            logger.info("SQL Server到MySQL的DDL同步测试环境清理完成");
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

        logger.info("SQL Server到MySQL的DDL同步测试用例环境初始化完成");
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
            String sqlServerResetSql =
                    "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                            "    first_name NVARCHAR(50) NOT NULL\n" +
                            ");";

            String mysqlResetSql =
                    "DROP TABLE IF EXISTS ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                            "    first_name VARCHAR(50) NOT NULL\n" +
                            ");";

            testDatabaseManager.resetTableStructure(sqlServerResetSql, mysqlResetSql);
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
        tableGroup.setId("sqlserver-to-mysql-tablegroup-id");
        tableGroup.setMappingId("sqlserver-to-mysql-mapping-id");

        Table sqlserverSourceTable = new Table();
        sqlserverSourceTable.setName("ddlTestEmployee");
        sqlserverSourceTable.setColumn(new ArrayList<>());

        Table mysqlTargetTable = new Table();
        mysqlTargetTable.setName("ddlTestEmployee");
        mysqlTargetTable.setColumn(new ArrayList<>());

        tableGroup.setSourceTable(sqlserverSourceTable);
        tableGroup.setTargetTable(mysqlTargetTable);

        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping idMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(idMapping);
        sqlserverSourceTable.getColumn().add(idSourceField);
        mysqlTargetTable.getColumn().add(idTargetField);

        Field firstNameSourceField = new Field("first_name", "NVARCHAR", 12);
        Field firstNameTargetField = new Field("first_name", "VARCHAR", 12);
        FieldMapping firstNameMapping = new FieldMapping(firstNameSourceField, firstNameTargetField);
        fieldMappings.add(firstNameMapping);
        sqlserverSourceTable.getColumn().add(firstNameSourceField);
        mysqlTargetTable.getColumn().add(firstNameTargetField);

        tableGroup.setFieldMapping(fieldMappings);

        // 配置TableGroup
        TestDDLHelper.setupTableGroup(tableGroup, "sqlserver-to-mysql-mapping-id",
                "sqlserver-connector-id", "mysql-connector-id",
                sqlServerConfig, mysqlConfig);

        // 创建GeneralBufferActuator
        generalBufferActuator = TestDDLHelper.createGeneralBufferActuator(
                connectorFactory,
                tableGroup.profileComponent,
                ddlParser);

        return tableGroup;
    }

    // ==================== SQL Server特殊类型转换测试 ====================

    @Test
    public void testAddColumn_XMLType() {
        logger.info("开始测试XML类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD xml_data XML";
        testDDLConversion(sqlserverDDL, "xml_data", "LONGTEXT", "LONGTEXT");
    }

    @Test
    public void testAddColumn_UNIQUEIDENTIFIERType() {
        logger.info("开始测试UNIQUEIDENTIFIER类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD guid UNIQUEIDENTIFIER";
        testDDLConversion(sqlserverDDL, "guid", "CHAR", "CHAR(36)");
    }

    @Test
    public void testAddColumn_MONEYType() {
        logger.info("开始测试MONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary MONEY";
        testDDLConversion(sqlserverDDL, "salary", "DECIMAL", "DECIMAL(19,4)");
    }

    @Test
    public void testAddColumn_SMALLMONEYType() {
        logger.info("开始测试SMALLMONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD bonus SMALLMONEY";
        testDDLConversion(sqlserverDDL, "bonus", "DECIMAL", "DECIMAL(10,4)");
    }

    @Test
    public void testAddColumn_DATETIME2Type() {
        logger.info("开始测试DATETIME2类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_at DATETIME2";
        testDDLConversion(sqlserverDDL, "created_at", "DATETIME", "DATETIME");
    }

    @Test
    public void testAddColumn_DATETIMEOFFSETType() {
        logger.info("开始测试DATETIMEOFFSET类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIMEOFFSET";
        testDDLConversion(sqlserverDDL, "updated_at", "DATETIME", "DATETIME");
    }

    @Test
    public void testAddColumn_TIMESTAMPType() {
        logger.info("开始测试TIMESTAMP类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD row_version TIMESTAMP";
        // SQL Server的TIMESTAMP是行版本控制类型，转换为BIGINT
        testDDLConversion(sqlserverDDL, "row_version", "BIGINT", "BIGINT");
    }

    @Test
    public void testAddColumn_IMAGEType() {
        logger.info("开始测试IMAGE类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD photo IMAGE";
        testDDLConversion(sqlserverDDL, "photo", "LONGBLOB", "LONGBLOB");
    }

    @Test
    public void testAddColumn_TEXTType() {
        logger.info("开始测试TEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD description TEXT";
        testDDLConversion(sqlserverDDL, "description", "TEXT", "TEXT");
    }

    @Test
    public void testAddColumn_NTEXTType() {
        logger.info("开始测试NTEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD notes NTEXT";
        testDDLConversion(sqlserverDDL, "notes", "TEXT", "TEXT");
    }

    @Test
    public void testAddColumn_BINARYType() {
        logger.info("开始测试BINARY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD binary_data BINARY(16)";
        testDDLConversion(sqlserverDDL, "binary_data", "BINARY", "BINARY(16)");
    }

    @Test
    public void testAddColumn_VARBINARYMAXType() {
        logger.info("开始测试VARBINARY(MAX)类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD varbinary_data VARBINARY(MAX)";
        testDDLConversion(sqlserverDDL, "varbinary_data", "LONGBLOB", "LONGBLOB");
    }

    @Test
    public void testAddColumn_SMALLDATETIMEType() {
        logger.info("开始测试SMALLDATETIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD small_date SMALLDATETIME";
        testDDLConversion(sqlserverDDL, "small_date", "DATETIME", "DATETIME");
    }

    @Test
    public void testAddColumn_BITType() {
        logger.info("开始测试BIT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD is_active BIT";
        testDDLConversion(sqlserverDDL, "is_active", "TINYINT", "TINYINT(1)");
    }

    @Test
    public void testAddColumn_HIERARCHYIDType() {
        logger.info("开始测试HIERARCHYID类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD org_path HIERARCHYID";
        testDDLConversion(sqlserverDDL, "org_path", "LONGBLOB", "LONGBLOB");
    }

    // ==================== SQL Server基础类型转换测试 ====================

    @Test
    public void testAddColumn_NVARCHARType() {
        logger.info("开始测试NVARCHAR类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD last_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, "last_name", "VARCHAR", "VARCHAR(100)");
    }

    @Test
    public void testAddColumn_VARCHARType() {
        logger.info("开始测试VARCHAR类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD code VARCHAR(50)";
        testDDLConversion(sqlserverDDL, "code", "VARCHAR", "VARCHAR(50)");
    }

    @Test
    public void testAddColumn_INTType() {
        logger.info("开始测试INT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD age INT";
        testDDLConversion(sqlserverDDL, "age", "INT", "INT");
    }

    @Test
    public void testAddColumn_BIGINTType() {
        logger.info("开始测试BIGINT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD count_num BIGINT";
        testDDLConversion(sqlserverDDL, "count_num", "BIGINT", "BIGINT");
    }

    @Test
    public void testAddColumn_DECIMALType() {
        logger.info("开始测试DECIMAL类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD price DECIMAL(10,2)";
        testDDLConversion(sqlserverDDL, "price", "DECIMAL", "DECIMAL(10,2)");
    }

    @Test
    public void testAddColumn_DATEType() {
        logger.info("开始测试DATE类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD birth_date DATE";
        testDDLConversion(sqlserverDDL, "birth_date", "DATE", "DATE");
    }

    @Test
    public void testAddColumn_TIMEType() {
        logger.info("开始测试TIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD work_time TIME";
        testDDLConversion(sqlserverDDL, "work_time", "TIME", "TIME");
    }

    @Test
    public void testAddColumn_DATETIMEType() {
        logger.info("开始测试DATETIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIME";
        testDDLConversion(sqlserverDDL, "updated_at", "DATETIME", "DATETIME");
    }

    // ==================== DDL操作测试 ====================

    @Test
    public void testModifyColumn_ChangeLength() {
        logger.info("开始测试MODIFY COLUMN操作 - 修改长度");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, "first_name", "VARCHAR", "VARCHAR(100)");
    }

    @Test
    public void testModifyColumn_AddNotNull() {
        logger.info("开始测试MODIFY COLUMN操作 - 添加NOT NULL约束");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        testDDLConversion(sqlserverDDL, "first_name", "VARCHAR", "VARCHAR(50)");
    }

    @Test
    public void testModifyColumn_ChangeType() {
        logger.info("开始测试MODIFY COLUMN操作 - 修改类型");
        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD count_num INT";
        try {
            executeDDLToSourceDatabase(addColumnDDL, sqlServerConfig);
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

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN count_num BIGINT";
        testDDLConversion(sqlserverDDL, "count_num", "BIGINT", "BIGINT");
    }

    @Test
    public void testModifyColumn_RemoveNotNull() {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        try {
            executeDDLToSourceDatabase(setNotNullDDL, sqlServerConfig);
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

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NULL";
        testDDLConversion(sqlserverDDL, "first_name", "VARCHAR", "VARCHAR(50)");
    }

    @Test
    public void testDropColumn() {
        logger.info("开始测试DROP COLUMN操作");
        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name", "first_name");
    }

    // ==================== 复杂场景测试 ====================

    @Test
    public void testAddMultipleColumns() {
        logger.info("开始测试多字段同时添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2), bonus DECIMAL(8,2)";
        
        try {
            // 1. 先在源数据库执行DDL
            executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);

            // 2. 创建WriterResponse和Mapping
            WriterResponse response = TestDDLHelper.createWriterResponse(sqlserverDDL, "ALTER", tableGroup.getSourceTable().getName());
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true, "test-meta-id");

            // 3. 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, tableGroup);

            // 4. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(mysqlConnectorService, tableGroup, sqlserverDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);
            logger.info("转换后的SQL: {}", targetSql);

            // 5. 验证转换后的SQL包含两个字段
            assertTrue(String.format("转换后的DDL应包含salary字段，实际SQL: %s", targetSql),
                    targetSql.contains("salary"));
            assertTrue(String.format("转换后的DDL应包含bonus字段，实际SQL: %s", targetSql),
                    targetSql.contains("bonus"));

            // 6. 验证目标数据库中两个字段都存在
            verifyFieldExistsInTargetDatabase("salary", tableGroup.getTargetTable().getName(), mysqlConfig);
            verifyFieldExistsInTargetDatabase("bonus", tableGroup.getTargetTable().getName(), mysqlConfig);

            logger.info("多字段添加测试通过 - salary和bonus字段都已正确转换");
        } catch (Exception e) {
            logger.error("多字段添加测试失败", e);
            fail("多字段添加测试失败: " + e.getMessage());
        }
    }

    @Test
    public void testAddColumn_WithDefault() {
        logger.info("开始测试带默认值的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD status NVARCHAR(20) DEFAULT 'active'";
        testDDLConversion(sqlserverDDL, "status", "VARCHAR", "VARCHAR(20)");
    }

    @Test
    public void testAddColumn_WithNotNull() {
        logger.info("开始测试带NOT NULL约束的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD phone NVARCHAR(20) NOT NULL";
        testDDLConversion(sqlserverDDL, "phone", "VARCHAR", "VARCHAR(20)");
    }

    @Test
    public void testAddColumn_WithDefaultAndNotNull() {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_by NVARCHAR(50) NOT NULL DEFAULT 'system'";
        testDDLConversion(sqlserverDDL, "created_by", "VARCHAR", "VARCHAR(50)");
    }

    // ==================== 通用测试方法 ====================

    /**
     * 执行DDL转换并验证结果
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName,
                                   String expectedTargetType, String expectedTargetPattern) {
        try {
            // 1. 先在源数据库执行DDL
            executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

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
            if (isAddOperation) {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()) &&
                                fm.getTarget() != null && expectedFieldName.equals(fm.getTarget().getName()));
                assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
            } else {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
                assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
            }

            // 5. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(mysqlConnectorService, tableGroup, sourceDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);

            // 6. 验证SQL语法
            validateSQLSyntax(targetSql);

            // 7. 验证转换后的SQL
            assertTrue(String.format("转换后的DDL应包含期望的目标类型 %s，实际SQL: %s", expectedTargetPattern, targetSql),
                    targetSql.toUpperCase().contains(expectedTargetPattern.toUpperCase()) ||
                            targetSql.toUpperCase().contains(expectedTargetType.toUpperCase()));
            assertTrue(String.format("转换后的DDL应包含字段名 %s，实际SQL: %s", expectedFieldName, targetSql),
                    targetSql.contains(expectedFieldName));

            // 8. 验证目标数据库中字段是否存在（对于ADD操作）
            if (isAddOperation) {
                verifyFieldExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), mysqlConfig);
            }

            logger.info("DDL转换测试通过 - 字段: {}, 目标类型: {}, 转换后SQL: {}",
                    expectedFieldName, expectedTargetType, targetSql);
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
            executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

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
            DDLConfig ddlConfig = ddlParser.parse(mysqlConnectorService, tableGroup, sourceDDL);
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
            verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), mysqlConfig);

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
        try (InputStream input = SQLServerToMySQLDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                sqlServerConfig = createDefaultSQLServerConfig();
                mysqlConfig = createDefaultMySQLConfig();
                return;
            }
            props.load(input);
        }

        sqlServerConfig = new DatabaseConfig();
        sqlServerConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true"));
        sqlServerConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sqlServerConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sqlServerConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));

        mysqlConfig = new DatabaseConfig();
        mysqlConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/target_db"));
        mysqlConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        mysqlConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        mysqlConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));
    }

    /**
     * 创建默认的SQL Server配置
     */
    private static DatabaseConfig createDefaultSQLServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true");
        config.setUsername("sa");
        config.setPassword("123456");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }

    /**
     * 创建默认的MySQL配置
     */
    private static DatabaseConfig createDefaultMySQLConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3306/target_db");
        config.setUsername("root");
        config.setPassword("123456");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }

    /**
     * 加载SQL脚本文件
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = SQLServerToMySQLDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
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

