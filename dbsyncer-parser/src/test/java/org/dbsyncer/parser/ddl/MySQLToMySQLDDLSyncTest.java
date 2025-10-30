package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
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

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化MySQL到MySQL的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

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

        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("mysql-test-tablegroup-id");
        testTableGroup.setMappingId("mysql-test-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理

        // 创建源表和目标表（MySQL）
        Table sourceTable = new Table();
        sourceTable.setName("ddlTestUserInfo");

        Table targetTable = new Table();
        targetTable.setName("ddlTestUserInfo");

        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);

        // 初始化字段映射
        FieldMapping idMapping = new FieldMapping(
                new Field("id", "INT", 4),
                new Field("id", "INT", 4)
        );

        FieldMapping usernameMapping = new FieldMapping(
                new Field("username", "VARCHAR", 12),
                new Field("username", "VARCHAR", 12)
        );

        FieldMapping emailMapping = new FieldMapping(
                new Field("email", "VARCHAR", 12),
                new Field("email", "VARCHAR", 12)
        );

        testTableGroup.setFieldMapping(java.util.Arrays.asList(idMapping, usernameMapping, emailMapping));

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
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getAddedFieldNames().contains("phone") : "新增字段列表应包含phone字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundPhoneMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "phone".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "phone".equals(mapping.getTarget().getName()));

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
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_DROP == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_DROP";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getDroppedFieldNames().contains("email") : "删除字段列表应包含email字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundEmailMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "email".equals(mapping.getSource().getName()));

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
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_MODIFY == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_MODIFY";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getModifiedFieldNames().contains("username") : "修改字段列表应包含username字段";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射仍然存在
            boolean foundUsernameMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "username".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "username".equals(mapping.getTarget().getName()));

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
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, testTableGroup, mysqlDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_CHANGE == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_CHANGE";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getChangedFieldNames().containsKey("username") &&
                    "user_name".equals(ddlConfig.getChangedFieldNames().get("username")) :
                    "变更字段映射应包含username到user_name的映射";

            // 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 验证字段映射更新
            boolean foundNewMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "username".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "user_name".equals(mapping.getTarget().getName()));

            boolean foundOldMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "username".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "username".equals(mapping.getTarget().getName()));

            assert foundNewMapping : "应找到username到user_name的字段映射";
            assert !foundOldMapping : "不应找到username到username的旧字段映射";

            logger.info("MySQL重命名字段同步测试通过");
        } catch (Exception e) {
            logger.error("MySQL重命名字段同步测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }
}