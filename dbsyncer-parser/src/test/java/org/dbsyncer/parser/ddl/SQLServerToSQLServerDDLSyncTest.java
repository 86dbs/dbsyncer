package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ddl.TestDDLHelper;
import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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

/**
 * SQL Server到SQL Server的DDL同步测试
 * 验证SQL Server数据库间的DDL同步功能
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class SQLServerToSQLServerDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerToSQLServerDDLSyncTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;
    private static ConnectorFactory connectorFactory;

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;
    private ConnectorService targetConnectorService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化SQL Server到SQL Server的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化ConnectorFactory（用于DDL解析器）
        connectorFactory = TestDDLHelper.createConnectorFactory();

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
    public void setUp() {
        ddlParser = new DDLParserImpl();

        // 初始化DDLParserImpl（初始化STRATEGIES）
        TestDDLHelper.initDDLParser(ddlParser);

        // 设置ConnectorFactory到DDLParserImpl
        TestDDLHelper.setConnectorFactory(ddlParser, connectorFactory);

        // 创建目标ConnectorService（用于DDL解析）
        targetConnectorService = TestDDLHelper.createConnectorService(targetConfig);

        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("sqlserver-test-tablegroup-id");
        testTableGroup.setMappingId("sqlserver-test-mapping-id");

        // 创建源表和目标表（SQL Server）
        Table sourceTable = new Table();
        sourceTable.setName("ddlTestEmployee");

        Table targetTable = new Table();
        targetTable.setName("ddlTestEmployee");

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

        // 配置TableGroup的profileComponent和Mapping信息
        TestDDLHelper.setupTableGroup(testTableGroup, "sqlserver-test-mapping-id",
                "sqlserver-test-source-connector-id", "sqlserver-test-target-connector-id",
                sourceConfig, targetConfig);

        logger.info("SQL Server到SQL Server的DDL同步测试用例环境初始化完成");
    }

    /**
     * 加载测试配置文件
     *
     * @throws IOException
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = SQLServerToSQLServerDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
     *
     * @return DatabaseConfig
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
     *
     * @param resourcePath 资源路径
     * @return SQL脚本内容
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = SQLServerToSQLServerDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath);
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

    /**
     * 测试SQL Server新增字段同步
     */
    @Test
    public void testSQLServerAddColumnSync() {
        logger.info("开始测试SQL Server新增字段同步");

        // SQL Server ADD COLUMN 语句
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2)";

        try {
            // 解析DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sqlServerDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert sqlServerDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getAddedFieldNames().contains("salary") : "新增字段列表应包含salary字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundSalaryMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "salary".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "salary".equals(mapping.getTarget().getName()));

            assert foundSalaryMapping : "应找到salary字段的映射";

            logger.info("SQL Server新增字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server新增字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试SQL Server删除字段同步
     */
    @Test
    public void testSQLServerDropColumnSync() {
        logger.info("开始测试SQL Server删除字段同步");

        // SQL Server DROP COLUMN 语句
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee DROP COLUMN department";

        try {
            // 解析DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sqlServerDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_DROP == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_DROP";
            assert sqlServerDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getDroppedFieldNames().contains("department") : "删除字段列表应包含department字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundDepartmentMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "department".equals(mapping.getSource().getName()));

            assert !foundDepartmentMapping : "不应找到department字段的映射";

            logger.info("SQL Server删除字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server删除字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试SQL Server修改字段同步
     */
    @Test
    public void testSQLServerAlterColumnSync() {
        logger.info("开始测试SQL Server修改字段同步");

        // SQL Server ALTER COLUMN 语句（注意SQL Server使用ALTER COLUMN而不是MODIFY COLUMN）
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";

        try {
            // 解析DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sqlServerDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_MODIFY == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_MODIFY";
            assert sqlServerDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getModifiedFieldNames().contains("first_name") : "修改字段列表应包含first_name字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射仍然存在
            boolean foundFirstNameMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "first_name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "first_name".equals(mapping.getTarget().getName()));

            assert foundFirstNameMapping : "应找到first_name字段的映射";

            logger.info("SQL Server修改字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server修改字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试SQL Server重命名字段同步
     */
    @Test
    public void testSQLServerRenameColumnSync() {
        logger.info("开始测试SQL Server重命名字段同步");

        // SQL Server CHANGE COLUMN 语句（注意SQL Server重命名字段需要使用标准的CHANGE语法进行测试）
        String sqlServerDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname NVARCHAR(50)";

        try {
            // 解析DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sqlServerDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_CHANGE == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_CHANGE";
            assert sqlServerDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getChangedFieldNames().containsKey("last_name") &&
                    "surname".equals(ddlConfig.getChangedFieldNames().get("last_name")) :
                    "变更字段映射应包含last_name到surname的映射";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundNewMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "surname".equals(mapping.getTarget().getName()));

            boolean foundOldMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "last_name".equals(mapping.getTarget().getName()));

            assert foundNewMapping : "应找到last_name到surname的字段映射";
            assert !foundOldMapping : "不应找到last_name到last_name的旧字段映射";

            logger.info("SQL Server重命名字段同步测试通过");
        } catch (Exception e) {
            logger.error("SQL Server重命名字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }
}