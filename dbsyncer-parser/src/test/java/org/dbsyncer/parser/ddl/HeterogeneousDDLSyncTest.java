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
 * 异构数据库DDL同步测试
 * 测试不同数据库类型间的DDL同步功能，涵盖所有特殊类型
 */
public class HeterogeneousDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(HeterogeneousDDLSyncTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig mssqlConfig;
    private static DatabaseConfig mysqlConfig;
    private static ConnectorFactory connectorFactory;

    private DDLParserImpl ddlParser; // 在testDDLConversion方法中使用
    private TableGroup sqlserverToMySQLTableGroup;
    private TableGroup mysqlToSQLServerTableGroup;
    private GeneralBufferActuator generalBufferActuator;
    private GeneralBufferActuator mysqlToSQLServerBufferActuator;

    // 全局定义的ConnectorService，避免重复创建
    private static ConnectorService sqlServerConnectorService;
    private static ConnectorService mysqlConnectorService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化异构数据库DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(mssqlConfig, mysqlConfig);

        // 初始化ConnectorFactory（用于DDL解析器）
        connectorFactory = TestDDLHelper.createConnectorFactory();

        // 初始化全局ConnectorService
        sqlServerConnectorService = TestDDLHelper.createConnectorService(mssqlConfig);
        mysqlConnectorService = TestDDLHelper.createConnectorService(mysqlConfig);

        // 初始化测试环境
        // 由于SQL Server不支持IF NOT EXISTS，需要分别处理
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

        logger.info("异构数据库DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理异构数据库DDL同步测试环境");

        try {
            // 清理测试环境
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("异构数据库DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws IOException {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        ddlParser = new DDLParserImpl();

        // 初始化DDLParserImpl（初始化STRATEGIES）
        TestDDLHelper.initDDLParser(ddlParser);

        // 设置ConnectorFactory到DDLParserImpl
        TestDDLHelper.setConnectorFactory(ddlParser, connectorFactory);

        // 初始化SQL Server到MySQL的TableGroup配置
        sqlserverToMySQLTableGroup = createSQLServerToMySQLTableGroup();

        // 初始化MySQL到SQL Server的TableGroup配置
        mysqlToSQLServerTableGroup = createMySQLToSQLServerTableGroup();

        logger.info("异构数据库DDL同步测试用例环境初始化完成");
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
            // 先删除表（如果存在），然后重新创建
            // 这样可以确保表结构完全重置，并且兼容SQL Server和MySQL
            ensureTableExists();
            logger.debug("测试数据库表结构重置完成");
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    /**
     * 确保测试表存在，如果不存在则创建
     * 由于测试可能从SQL Server到MySQL或MySQL到SQL Server，需要在两个数据库中都创建表
     */
    private void ensureTableExists() {
        logger.debug("确保测试表存在");
        try {
            // SQL Server: 先删除表（如果存在），然后创建
            // 注意：testDatabaseManager的第一个参数是SQL Server（mssqlConfig），第二个是MySQL（mysqlConfig）
            String sqlServerDropAndCreate =
                    "IF OBJECT_ID('ddlTestEmployee', 'U') IS NOT NULL DROP TABLE ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                            "    first_name NVARCHAR(50) NOT NULL\n" +
                            ");";

            // MySQL: 先删除表（如果存在），然后创建
            String mysqlDropAndCreate =
                    "DROP TABLE IF EXISTS ddlTestEmployee;\n" +
                            "CREATE TABLE ddlTestEmployee (\n" +
                            "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
                            "    first_name VARCHAR(50) NOT NULL\n" +
                            ");";

            // 在SQL Server和MySQL中都创建表（因为测试可能从任一方向进行）
            // testDatabaseManager的第一个参数是SQL Server，第二个是MySQL
            testDatabaseManager.resetTableStructure(sqlServerDropAndCreate, mysqlDropAndCreate);
        } catch (Exception e) {
            logger.error("确保测试表存在失败", e);
            // 不抛出异常，避免影响测试，但记录错误
            // 如果表创建失败，后续测试会失败，这样可以清楚地看到问题
        }
    }

    /**
     * 创建SQL Server到MySQL的TableGroup配置
     */
    private TableGroup createSQLServerToMySQLTableGroup() {
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
                mssqlConfig, mysqlConfig);

        // 创建GeneralBufferActuator
        generalBufferActuator = TestDDLHelper.createGeneralBufferActuator(
                connectorFactory,
                tableGroup.profileComponent,
                ddlParser);

        return tableGroup;
    }

    /**
     * 创建MySQL到SQL Server的TableGroup配置
     */
    private TableGroup createMySQLToSQLServerTableGroup() {
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

        // 配置TableGroup
        // 注意：sourceConfig是SQL Server，targetConfig是MySQL
        // MySQL到SQL Server：源是MySQL(targetConfig)，目标是SQL Server(sourceConfig)
        TestDDLHelper.setupTableGroup(tableGroup, "mysql-to-sqlserver-mapping-id",
                "mysql-connector-id", "sqlserver-connector-id",
                mysqlConfig, mssqlConfig);

        // 创建MySQL到SQL Server的GeneralBufferActuator
        mysqlToSQLServerBufferActuator = TestDDLHelper.createGeneralBufferActuator(
                connectorFactory,
                tableGroup.profileComponent,
                ddlParser);

        return tableGroup;
    }

    // ========== SQL Server特殊类型测试 ==========

    /**
     * 测试SQL Server到MySQL - XML类型转换
     */
    @Test
    public void testSQLServerToMySQL_XMLType() {
        logger.info("开始测试SQL Server到MySQL的XML类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD xml_data XML";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "xml_data",
                "SqlServer", "MySQL", mysqlConnectorService, "LONGTEXT", "LONGTEXT");
    }

    /**
     * 测试SQL Server到MySQL - UNIQUEIDENTIFIER类型转换
     */
    @Test
    public void testSQLServerToMySQL_UNIQUEIDENTIFIERType() {
        logger.info("开始测试SQL Server到MySQL的UNIQUEIDENTIFIER类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD guid UNIQUEIDENTIFIER";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "guid",
                "SqlServer", "MySQL", mysqlConnectorService, "CHAR", "CHAR(36)");
    }

    /**
     * 测试SQL Server到MySQL - MONEY类型转换
     */
    @Test
    public void testSQLServerToMySQL_MONEYType() {
        logger.info("开始测试SQL Server到MySQL的MONEY类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary MONEY";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "salary",
                "SqlServer", "MySQL", mysqlConnectorService, "DECIMAL", "DECIMAL(19,4)");
    }

    /**
     * 测试SQL Server到MySQL - SMALLMONEY类型转换
     */
    @Test
    public void testSQLServerToMySQL_SMALLMONEYType() {
        logger.info("开始测试SQL Server到MySQL的SMALLMONEY类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD bonus SMALLMONEY";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "bonus",
                "SqlServer", "MySQL", mysqlConnectorService, "DECIMAL", "DECIMAL(10,4)");
    }

    /**
     * 测试SQL Server到MySQL - DATETIME2类型转换
     */
    @Test
    public void testSQLServerToMySQL_DATETIME2Type() {
        logger.info("开始测试SQL Server到MySQL的DATETIME2类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_at DATETIME2";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "created_at",
                "SqlServer", "MySQL", mysqlConnectorService, "DATETIME", "DATETIME");
    }

    /**
     * 测试SQL Server到MySQL - DATETIMEOFFSET类型转换
     */
    @Test
    public void testSQLServerToMySQL_DATETIMEOFFSETType() {
        logger.info("开始测试SQL Server到MySQL的DATETIMEOFFSET类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIMEOFFSET";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "updated_at",
                "SqlServer", "MySQL", mysqlConnectorService, "DATETIME", "DATETIME");
    }

    /**
     * 测试SQL Server到MySQL - TIMESTAMP类型转换
     */
    @Test
    public void testSQLServerToMySQL_TIMESTAMPType() {
        logger.info("开始测试SQL Server到MySQL的TIMESTAMP类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD row_version TIMESTAMP";
        // SQL Server的TIMESTAMP是行版本控制类型（8字节二进制），转换为long类型，在MySQL中映射为BIGINT
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "row_version",
                "SqlServer", "MySQL", mysqlConnectorService, "BIGINT", "BIGINT");
    }

    /**
     * 测试SQL Server到MySQL - IMAGE类型转换
     */
    @Test
    public void testSQLServerToMySQL_IMAGEType() {
        logger.info("开始测试SQL Server到MySQL的IMAGE类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD photo IMAGE";
        // SQL Server的IMAGE类型最大2GB，应映射到MySQL的LONGBLOB
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "photo",
                "SqlServer", "MySQL", mysqlConnectorService, "LONGBLOB", "LONGBLOB");
    }

    /**
     * 测试SQL Server到MySQL - TEXT类型转换
     */
    @Test
    public void testSQLServerToMySQL_TEXTType() {
        logger.info("开始测试SQL Server到MySQL的TEXT类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD description TEXT";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "description",
                "SqlServer", "MySQL", mysqlConnectorService, "TEXT", "TEXT");
    }

    /**
     * 测试SQL Server到MySQL - NTEXT类型转换
     */
    @Test
    public void testSQLServerToMySQL_NTEXTType() {
        logger.info("开始测试SQL Server到MySQL的NTEXT类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD notes NTEXT";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "notes",
                "SqlServer", "MySQL", mysqlConnectorService, "TEXT", "TEXT");
    }

    /**
     * 测试SQL Server到MySQL - BINARY类型转换
     */
    @Test
    public void testSQLServerToMySQL_BINARYTypes() {
        logger.info("开始测试SQL Server到MySQL的BINARY类型转换");

        String sqlserverDDL1 = "ALTER TABLE ddlTestEmployee ADD binary_data BINARY(16)";
        testDDLConversion(sqlserverDDL1, sqlserverToMySQLTableGroup, "binary_data",
                "SqlServer", "MySQL", mysqlConnectorService, "BINARY", "BINARY(16)");

        String sqlserverDDL2 = "ALTER TABLE ddlTestEmployee ADD varbinary_data VARBINARY(MAX)";
        // VARBINARY(MAX) 应该映射到标准类型 BLOB，然后转换为 MySQL 的 LONGBLOB（因为 MAX 表示 2GB）
        testDDLConversion(sqlserverDDL2, sqlserverToMySQLTableGroup, "varbinary_data",
                "SqlServer", "MySQL", mysqlConnectorService, "LONGBLOB", "LONGBLOB");
    }

    /**
     * 测试SQL Server到MySQL - SMALLDATETIME类型转换
     */
    @Test
    public void testSQLServerToMySQL_SMALLDATETIMEType() {
        logger.info("开始测试SQL Server到MySQL的SMALLDATETIME类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD small_date SMALLDATETIME";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "small_date",
                "SqlServer", "MySQL", mysqlConnectorService, "DATETIME", "DATETIME");
    }

    /**
     * 测试SQL Server到MySQL - BIT类型转换
     */
    @Test
    public void testSQLServerToMySQL_BITType() {
        logger.info("开始测试SQL Server到MySQL的BIT类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD is_active BIT";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "is_active",
                "SqlServer", "MySQL", mysqlConnectorService, "TINYINT", "TINYINT(1)");
    }

    /**
     * 测试SQL Server到MySQL - HIERARCHYID类型转换
     */
    @Test
    public void testSQLServerToMySQL_HIERARCHYIDType() {
        logger.info("开始测试SQL Server到MySQL的HIERARCHYID类型转换");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD org_path HIERARCHYID";
        // HIERARCHYID 类型映射到标准类型 BLOB（columnSize = 2GB），在 MySQL 中转换为 LONGBLOB
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "org_path",
                "SqlServer", "MySQL", mysqlConnectorService, "LONGBLOB", "LONGBLOB");
    }

    // ========== MySQL特殊类型测试 ==========

    /**
     * 测试MySQL到SQL Server - ENUM类型转换
     */
    @Test
    public void testMySQLToSQLServer_ENUMType() {
        logger.info("开始测试MySQL到SQL Server的ENUM类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN status ENUM('active','inactive','pending')";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "status",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR");
    }

    /**
     * 测试MySQL到SQL Server - SET类型转换
     */
    @Test
    public void testMySQLToSQLServer_SETType() {
        logger.info("开始测试MySQL到SQL Server的SET类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN tags SET('tag1','tag2','tag3')";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "tags",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR");
    }

    /**
     * 测试MySQL到SQL Server - JSON类型转换
     */
    @Test
    public void testMySQLToSQLServer_JSONType() {
        logger.info("开始测试MySQL到SQL Server的JSON类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN metadata JSON";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "metadata",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - YEAR类型转换
     */
    @Test
    public void testMySQLToSQLServer_YEARType() {
        logger.info("开始测试MySQL到SQL Server的YEAR类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN birth_year YEAR";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "birth_year",
                "MySQL", "SqlServer", sqlServerConnectorService, "SMALLINT", "SMALLINT");
    }

    /**
     * 测试MySQL到SQL Server - GEOMETRY类型转换
     */
    @Test
    public void testMySQLToSQLServer_GEOMETRYType() {
        logger.info("开始测试MySQL到SQL Server的GEOMETRY类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN location GEOMETRY";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "location",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - POINT类型转换
     */
    @Test
    public void testMySQLToSQLServer_POINTType() {
        logger.info("开始测试MySQL到SQL Server的POINT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN coordinates POINT";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "coordinates",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - LINESTRING类型转换
     */
    @Test
    public void testMySQLToSQLServer_LINESTRINGType() {
        logger.info("开始测试MySQL到SQL Server的LINESTRING类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN route LINESTRING";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "route",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - POLYGON类型转换
     */
    @Test
    public void testMySQLToSQLServer_POLYGONType() {
        logger.info("开始测试MySQL到SQL Server的POLYGON类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN area POLYGON";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "area",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - MULTIPOINT类型转换
     */
    @Test
    public void testMySQLToSQLServer_MULTIPOINTType() {
        logger.info("开始测试MySQL到SQL Server的MULTIPOINT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN points MULTIPOINT";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "points",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - MULTILINESTRING类型转换
     */
    @Test
    public void testMySQLToSQLServer_MULTILINESTRINGType() {
        logger.info("开始测试MySQL到SQL Server的MULTILINESTRING类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN routes MULTILINESTRING";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "routes",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - MULTIPOLYGON类型转换
     */
    @Test
    public void testMySQLToSQLServer_MULTIPOLYGONType() {
        logger.info("开始测试MySQL到SQL Server的MULTIPOLYGON类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN regions MULTIPOLYGON";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "regions",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - GEOMETRYCOLLECTION类型转换
     */
    @Test
    public void testMySQLToSQLServer_GEOMETRYCOLLECTIONType() {
        logger.info("开始测试MySQL到SQL Server的GEOMETRYCOLLECTION类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN collection GEOMETRYCOLLECTION";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "collection",
                "MySQL", "SqlServer", sqlServerConnectorService, "VARBINARY", "VARBINARY(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - TINYTEXT类型转换
     */
    @Test
    public void testMySQLToSQLServer_TINYTEXTType() {
        logger.info("开始测试MySQL到SQL Server的TINYTEXT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN short_text TINYTEXT";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "short_text",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(255)");
    }

    /**
     * 测试MySQL到SQL Server - MEDIUMTEXT类型转换
     */
    @Test
    public void testMySQLToSQLServer_MEDIUMTEXTType() {
        logger.info("开始测试MySQL到SQL Server的MEDIUMTEXT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN medium_text MEDIUMTEXT";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "medium_text",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - LONGTEXT类型转换
     */
    @Test
    public void testMySQLToSQLServer_LONGTEXTType() {
        logger.info("开始测试MySQL到SQL Server的LONGTEXT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN long_text LONGTEXT";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "long_text",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(MAX)");
    }

    /**
     * 测试MySQL到SQL Server - BIT类型转换
     */
    @Test
    public void testMySQLToSQLServer_BITType() {
        logger.info("开始测试MySQL到SQL Server的BIT类型转换");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN is_verified BIT(1)";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "is_verified",
                "MySQL", "SqlServer", sqlServerConnectorService, "TINYINT", "TINYINT");
    }

    // ========== DDL操作测试 ==========

    /**
     * 测试SQL Server到MySQL - MODIFY COLUMN操作
     */
    @Test
    public void testSQLServerToMySQL_ModifyColumn() {
        logger.info("开始测试SQL Server到MySQL的MODIFY COLUMN操作");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "first_name",
                "SqlServer", "MySQL", mysqlConnectorService, "VARCHAR", "VARCHAR(100)");
    }

    /**
     * 测试MySQL到SQL Server - CHANGE COLUMN操作
     */
    @Test
    public void testMySQLToSQLServer_ChangeColumn() {
        logger.info("开始测试MySQL到SQL Server的CHANGE COLUMN操作");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN first_name given_name VARCHAR(100)";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "given_name",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(100)");
    }

    /**
     * 测试SQL Server到MySQL - DROP COLUMN操作
     */
    @Test
    public void testSQLServerToMySQL_DropColumn() {
        logger.info("开始测试SQL Server到MySQL的DROP COLUMN操作");

        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name",
                sqlserverToMySQLTableGroup, "first_name",
                "SqlServer", "MySQL", mysqlConnectorService);
    }

    // ========== 复杂场景测试 ==========

    /**
     * 测试多字段同时添加
     */
    @Test
    public void testSQLServerToMySQL_AddMultipleColumns() {
        logger.info("开始测试SQL Server到MySQL的多字段添加");

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2), bonus DECIMAL(8,2)";
        testDDLConversion(sqlserverDDL, sqlserverToMySQLTableGroup, "salary",
                "SqlServer", "MySQL", mysqlConnectorService, "DECIMAL", "DECIMAL(10,2)");
        // 还需要验证bonus字段也被正确转换
    }

    /**
     * 测试带约束的字段添加
     */
    @Test
    public void testMySQLToSQLServer_AddColumnWithConstraint() {
        logger.info("开始测试MySQL到SQL Server的带约束字段添加");

        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN email VARCHAR(100) NOT NULL DEFAULT 'unknown@example.com'";
        testDDLConversion(mysqlDDL, mysqlToSQLServerTableGroup, "email",
                "MySQL", "SqlServer", sqlServerConnectorService, "NVARCHAR", "NVARCHAR(100)");
    }

    // ========== 通用DDL转换测试方法 ==========

    /**
     * 执行DDL转换并验证结果
     *
     * @param sourceDDL              源数据库DDL语句
     * @param tableGroup             TableGroup配置
     * @param expectedFieldName      期望的字段名
     * @param sourceDbType           源数据库类型（"SqlServer" 或 "MySQL"）
     * @param targetDbType           目标数据库类型（"SqlServer" 或 "MySQL"）
     * @param targetConnectorService 目标数据库的ConnectorService（用于DDL解析和验证）
     * @param expectedTargetType     期望转换后的目标类型（如 "TEXT", "NVARCHAR", "DECIMAL"等）
     * @param expectedTargetPattern  期望的目标类型模式（用于SQL验证，可能包含长度等，如 "TEXT", "NVARCHAR(50)", "DECIMAL(19,4)"）
     */
    private void testDDLConversion(String sourceDDL, TableGroup tableGroup, String expectedFieldName,
                                   String sourceDbType, String targetDbType,
                                   ConnectorService targetConnectorService,
                                   String expectedTargetType, String expectedTargetPattern) {
        try {
            // 1. 先在源数据库执行DDL（模拟真实的DDL变更）
            DatabaseConfig sourceConfig = getSourceConfig(tableGroup);
            executeDDLToSourceDatabase(sourceDDL, sourceConfig);

            // 2. 创建WriterResponse
            WriterResponse response = TestDDLHelper.createWriterResponse(sourceDDL, "ALTER", tableGroup.getSourceTable().getName());

            // 3. 创建Mapping
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true,
                    "test-meta-id");

            // 4. 根据TableGroup选择正确的BufferActuator
            GeneralBufferActuator actuator = (tableGroup == mysqlToSQLServerTableGroup)
                    ? mysqlToSQLServerBufferActuator
                    : generalBufferActuator;

            // 5. 调用完整的DDL处理流程（解析DDL → 执行DDL到目标数据库 → 刷新表结构 → 更新字段映射）
            actuator.parseDDl(response, mapping, tableGroup);

            // 6. 验证字段映射是否更新
            boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
            if (isAddOperation) {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fieldMapping -> fieldMapping.getSource() != null &&
                                expectedFieldName.equals(fieldMapping.getSource().getName()) &&
                                fieldMapping.getTarget() != null &&
                                expectedFieldName.equals(fieldMapping.getTarget().getName()));

                assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
            } else {
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fieldMapping -> fieldMapping.getSource() != null &&
                                expectedFieldName.equals(fieldMapping.getSource().getName()));

                assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);
            }

            // 7. 验证DDL转换结果（通过解析DDLConfig）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, tableGroup, sourceDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            assertNotNull("DDL配置应包含操作类型", ddlConfig.getDdlOperationEnum());

            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);

            // 8. 验证SQL语法正确性
            validateSQLSyntax(targetSql);

            // 9. 验证转换后的SQL是否包含期望的目标类型
            assertTrue(String.format("转换后的DDL应包含期望的目标类型 %s，实际SQL: %s", expectedTargetPattern, targetSql),
                    targetSql.toUpperCase().contains(expectedTargetPattern.toUpperCase()) ||
                            targetSql.toUpperCase().contains(expectedTargetType.toUpperCase()));

            // 10. 验证字段名存在于转换后的SQL中
            assertTrue(String.format("转换后的DDL应包含字段名 %s，实际SQL: %s", expectedFieldName, targetSql),
                    targetSql.contains(expectedFieldName));

            // 11. 验证目标数据库中字段是否真实存在（对于ADD操作）
            if (isAddOperation) {
                DatabaseConfig targetConfig = getTargetConfig(tableGroup);
                verifyFieldExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), targetConfig);
            }

            logger.info("DDL转换测试通过 - 字段: {}, 源类型: {}, 目标类型: {}, 转换后SQL: {}",
                    expectedFieldName, sourceDbType, targetDbType, targetSql);
        } catch (Exception e) {
            logger.error("DDL转换测试失败 - 字段: {}, 源类型: {}, 目标类型: {}",
                    expectedFieldName, sourceDbType, targetDbType, e);
            fail("DDL转换测试失败: " + e.getMessage());
        }
    }

    /**
     * 测试DDL DROP操作的通用方法
     */
    private void testDDLDropOperation(String sourceDDL, TableGroup tableGroup, String expectedFieldName,
                                      String sourceDbType, String targetDbType,
                                      ConnectorService targetConnectorService) {
        try {
            // 1. 先在源数据库执行DDL（模拟真实的DDL变更）
            DatabaseConfig sourceConfig = getSourceConfig(tableGroup);
            executeDDLToSourceDatabase(sourceDDL, sourceConfig);

            // 2. 创建WriterResponse
            WriterResponse response = TestDDLHelper.createWriterResponse(sourceDDL, "ALTER", tableGroup.getSourceTable().getName());

            // 3. 创建Mapping
            Mapping mapping = TestDDLHelper.createMapping(
                    tableGroup.getMappingId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId(),
                    tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId(),
                    true,
                    "test-meta-id");

            // 4. 根据TableGroup选择正确的BufferActuator
            GeneralBufferActuator actuator = (tableGroup == mysqlToSQLServerTableGroup)
                    ? mysqlToSQLServerBufferActuator
                    : generalBufferActuator;

            // 5. 调用完整的DDL处理流程（解析DDL → 执行DDL到目标数据库 → 刷新表结构 → 更新字段映射）
            actuator.parseDDl(response, mapping, tableGroup);

            // 6. 验证字段映射是否已移除
            boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null &&
                            expectedFieldName.equals(fieldMapping.getSource().getName()));

            assertFalse("应移除字段 " + expectedFieldName + " 的映射", foundFieldMapping);

            // 7. 验证DDL转换结果
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, tableGroup, sourceDDL);
            assertNotNull("DDL配置不应为空", ddlConfig);
            assertNotNull("DDL配置应包含操作类型", ddlConfig.getDdlOperationEnum());

            String targetSql = ddlConfig.getSql();
            assertNotNull("DDL配置应包含SQL语句", targetSql);
            validateSQLSyntax(targetSql);

            // 8. 验证转换后的SQL是否包含DROP COLUMN关键字和字段名
            assertTrue(String.format("转换后的DDL应包含DROP COLUMN关键字，实际SQL: %s", targetSql),
                    targetSql.toUpperCase().contains("DROP COLUMN"));
            assertTrue(String.format("转换后的DDL应包含字段名 %s，实际SQL: %s", expectedFieldName, targetSql),
                    targetSql.contains(expectedFieldName));

            // 9. 验证目标数据库中字段是否已被删除
            DatabaseConfig targetConfig = getTargetConfig(tableGroup);
            verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), targetConfig);

            logger.info("DDL DROP操作测试通过 - 字段: {}, 源类型: {}, 目标类型: {}, 转换后SQL: {}",
                    expectedFieldName, sourceDbType, targetDbType, targetSql);
        } catch (Exception e) {
            logger.error("DDL DROP操作测试失败 - 字段: {}, 源类型: {}, 目标类型: {}",
                    expectedFieldName, sourceDbType, targetDbType, e);
            fail("DDL DROP操作测试失败: " + e.getMessage());
        }
    }

    /**
     * 验证转换后的SQL语法正确性
     */
    private void validateSQLSyntax(String sql) {
        try {
            CCJSqlParserUtil.parse(sql);
            logger.debug("SQL语法验证通过: {}", sql);
        } catch (JSQLParserException e) {
            logger.error("SQL语法验证失败: {}", sql, e);
            fail("转换后的SQL语法不正确: " + sql);
        }
    }

    // ========== 辅助方法 ==========

    /**
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = HeterogeneousDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                mssqlConfig = createDefaultSQLServerConfig();
                mysqlConfig = createDefaultMySQLConfig();
                return;
            }
            props.load(input);
        }

        // 创建源数据库配置(SQL Server)
        mssqlConfig = new DatabaseConfig();
        mssqlConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true"));
        mssqlConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        mssqlConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        mssqlConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));

        // 创建目标数据库配置(MySQL)
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
        try (InputStream input = HeterogeneousDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
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
     * 获取源数据库配置
     */
    private DatabaseConfig getSourceConfig(TableGroup tableGroup) {
        String sourceConnectorId = tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getSourceConnectorId();
        return (DatabaseConfig) tableGroup.profileComponent.getConnector(sourceConnectorId).getConfig();
    }

    /**
     * 获取目标数据库配置
     */
    private DatabaseConfig getTargetConfig(TableGroup tableGroup) {
        String targetConnectorId = tableGroup.profileComponent.getMapping(tableGroup.getMappingId()).getTargetConnectorId();
        return (DatabaseConfig) tableGroup.profileComponent.getConnector(targetConnectorId).getConfig();
    }

    /**
     * 在源数据库执行DDL（模拟真实的DDL变更）
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
     * 验证目标数据库中字段是否不存在（用于DROP操作）
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
