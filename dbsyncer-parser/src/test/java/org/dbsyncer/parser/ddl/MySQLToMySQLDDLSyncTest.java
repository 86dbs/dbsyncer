package org.dbsyncer.parser.ddl;

import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.flush.impl.GeneralBufferActuator;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
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
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * MySQL到MySQL的DDL同步测试
 * 验证同类型数据库间的DDL同步功能
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class MySQLToMySQLDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(MySQLToMySQLDDLSyncTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;
    private static ConnectorFactory connectorFactory;

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;
    private GeneralBufferActuator generalBufferActuator;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化MySQL到MySQL的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化ConnectorFactory（用于DDL解析器）
        connectorFactory = TestDDLHelper.createConnectorFactory();

        // 初始化测试环境
        String initSql = loadSqlScript("ddl/init-test-data.sql");
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("MySQL到MySQL的DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理MySQL到MySQL的DDL同步测试环境");

        try {
            // 清理测试环境
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("MySQL到MySQL的DDL同步测试环境清理完成");
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

        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("mysql-test-tablegroup-id");
        testTableGroup.setMappingId("mysql-test-mapping-id");

        // 创建源表和目标表（MySQL）
        Table sourceTable = new Table();
        sourceTable.setName("ddlTestUserInfo");
        sourceTable.setColumn(new ArrayList<>());

        Table targetTable = new Table();
        targetTable.setName("ddlTestUserInfo");
        targetTable.setColumn(new ArrayList<>());

        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);

        // 初始化字段映射
        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping idMapping = new FieldMapping(idSourceField, idTargetField);
        sourceTable.getColumn().add(idSourceField);
        targetTable.getColumn().add(idTargetField);

        Field usernameSourceField = new Field("username", "VARCHAR", 12);
        Field usernameTargetField = new Field("username", "VARCHAR", 12);
        FieldMapping usernameMapping = new FieldMapping(usernameSourceField, usernameTargetField);
        sourceTable.getColumn().add(usernameSourceField);
        targetTable.getColumn().add(usernameTargetField);

        Field emailSourceField = new Field("email", "VARCHAR", 12);
        Field emailTargetField = new Field("email", "VARCHAR", 12);
        FieldMapping emailMapping = new FieldMapping(emailSourceField, emailTargetField);
        sourceTable.getColumn().add(emailSourceField);
        targetTable.getColumn().add(emailTargetField);

        testTableGroup.setFieldMapping(java.util.Arrays.asList(idMapping, usernameMapping, emailMapping));

        // 配置TableGroup的profileComponent和Mapping信息
        TestDDLHelper.setupTableGroup(testTableGroup, "mysql-test-mapping-id",
                "mysql-test-source-connector-id", "mysql-test-target-connector-id",
                sourceConfig, targetConfig);

        // 创建并配置GeneralBufferActuator（用于调用完整的parseDDl流程）
        generalBufferActuator = TestDDLHelper.createGeneralBufferActuator(
                connectorFactory,
                testTableGroup.profileComponent,
                ddlParser);

        logger.info("MySQL到MySQL的DDL同步测试用例环境初始化完成");
    }

    /**
     * 加载测试配置文件
     *
     * @throws IOException
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = MySQLToMySQLDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
     *
     * @return DatabaseConfig
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
     *
     * @param resourcePath 资源路径
     * @return SQL脚本内容
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = MySQLToMySQLDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath);
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
     * 测试MySQL新增字段同步
     */
    @Test
    public void testMySQLAddColumnSync() {
        logger.info("开始测试MySQL新增字段同步");

        // MySQL ADD COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestUserInfo ADD COLUMN phone VARCHAR(20) AFTER email";

        try {
            // 使用GeneralBufferActuator.parseDDl()调用完整的真实流程
            WriterResponse response = TestDDLHelper.createWriterResponse(mysqlDDL, "ALTER", "ddlTestUserInfo");
            Mapping mapping = TestDDLHelper.createMapping("mysql-test-mapping-id",
                    "mysql-test-source-connector-id", "mysql-test-target-connector-id", true, "mysql-test-meta-id");

            // 调用完整的DDL处理流程（解析DDL → 执行DDL → 刷新表结构 → 更新字段映射）
            generalBufferActuator.parseDDl(response, mapping, testTableGroup);

            // 验证字段映射更新
            boolean foundPhoneMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null && "phone".equals(fieldMapping.getSource().getName()) &&
                            fieldMapping.getTarget() != null && "phone".equals(fieldMapping.getTarget().getName()));

            assert foundPhoneMapping : "应找到phone字段的映射";

            logger.info("MySQL新增字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL新增字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL删除字段同步
     */
    @Test
    public void testMySQLDropColumnSync() {
        logger.info("开始测试MySQL删除字段同步");

        // MySQL DROP COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestUserInfo DROP COLUMN email";

        try {
            // 使用GeneralBufferActuator.parseDDl()调用完整的真实流程
            WriterResponse response = TestDDLHelper.createWriterResponse(mysqlDDL, "ALTER", "ddlTestUserInfo");
            Mapping mapping = TestDDLHelper.createMapping("mysql-test-mapping-id",
                    "mysql-test-source-connector-id", "mysql-test-target-connector-id", true, "mysql-test-meta-id");

            // 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, testTableGroup);

            // 验证字段映射更新
            boolean foundEmailMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null && "email".equals(fieldMapping.getSource().getName()));

            assert !foundEmailMapping : "不应找到email字段的映射";

            logger.info("MySQL删除字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL删除字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL修改字段同步
     */
    @Test
    public void testMySQLModifyColumnSync() {
        logger.info("开始测试MySQL修改字段同步");

        // MySQL MODIFY COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestUserInfo MODIFY COLUMN username VARCHAR(100) NOT NULL";

        try {
            // 使用GeneralBufferActuator.parseDDl()调用完整的真实流程
            WriterResponse response = TestDDLHelper.createWriterResponse(mysqlDDL, "ALTER", "ddlTestUserInfo");
            Mapping mapping = TestDDLHelper.createMapping("mysql-test-mapping-id",
                    "mysql-test-source-connector-id", "mysql-test-target-connector-id", true, "mysql-test-meta-id");

            // 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, testTableGroup);

            // 验证字段映射仍然存在
            boolean foundUsernameMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null && "username".equals(fieldMapping.getSource().getName()) &&
                            fieldMapping.getTarget() != null && "username".equals(fieldMapping.getTarget().getName()));

            assert foundUsernameMapping : "应找到username字段的映射";

            logger.info("MySQL修改字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL修改字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL重命名字段同步
     */
    @Test
    public void testMySQLChangeColumnSync() {
        logger.info("开始测试MySQL重命名字段同步");

        // MySQL CHANGE COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestUserInfo CHANGE COLUMN username user_name VARCHAR(50)";

        try {
            // 使用GeneralBufferActuator.parseDDl()调用完整的真实流程
            WriterResponse response = TestDDLHelper.createWriterResponse(mysqlDDL, "ALTER", "ddlTestUserInfo");
            Mapping mapping = TestDDLHelper.createMapping("mysql-test-mapping-id",
                    "mysql-test-source-connector-id", "mysql-test-target-connector-id", true, "mysql-test-meta-id");

            // 调用完整的DDL处理流程
            generalBufferActuator.parseDDl(response, mapping, testTableGroup);

            // 验证字段映射更新
            boolean foundNewMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null && "username".equals(fieldMapping.getSource().getName()) &&
                            fieldMapping.getTarget() != null && "user_name".equals(fieldMapping.getTarget().getName()));

            boolean foundOldMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(fieldMapping -> fieldMapping.getSource() != null && "username".equals(fieldMapping.getSource().getName()) &&
                            fieldMapping.getTarget() != null && "username".equals(fieldMapping.getTarget().getName()));

            assert foundNewMapping : "应找到username到user_name的字段映射";
            assert !foundOldMapping : "不应找到username到username的旧字段映射";

            logger.info("MySQL重命名字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL重命名字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }
}