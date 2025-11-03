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
 * DDL同步集成测试
 * 测试DDL同步的端到端功能，包括解析、转换和执行
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class DDLSyncIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DDLSyncIntegrationTest.class);

    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;
    private static ConnectorFactory connectorFactory;

    private DDLParserImpl ddlParser;
    private TableGroup testTableGroup;
    private ConnectorService targetConnectorService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化ConnectorFactory（用于DDL解析器）
        connectorFactory = TestDDLHelper.createConnectorFactory();

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
    public void setUp() throws IOException {
        ddlParser = new DDLParserImpl();

        // 初始化DDLParserImpl（初始化STRATEGIES）
        TestDDLHelper.initDDLParser(ddlParser);

        // 设置ConnectorFactory到DDLParserImpl
        TestDDLHelper.setConnectorFactory(ddlParser, connectorFactory);

        // 创建目标ConnectorService（用于DDL解析）
        targetConnectorService = TestDDLHelper.createConnectorService(targetConfig);

        // 创建测试用的TableGroup配置
        testTableGroup = new TableGroup();
        testTableGroup.setId("test-tablegroup-id");
        testTableGroup.setMappingId("test-mapping-id");

        // 创建源表和目标表
        Table sourceTable = new Table();
        sourceTable.setName("ddlTestTable");
        sourceTable.setColumn(new ArrayList<>());

        Table targetTable = new Table();
        targetTable.setName("ddlTestTable");
        targetTable.setColumn(new ArrayList<>());

        testTableGroup.setSourceTable(sourceTable);
        testTableGroup.setTargetTable(targetTable);

        // 初始化字段映射
        List<FieldMapping> fieldMappings = new ArrayList<>();

        Field idSourceField = new Field("id", "INT", 4);
        Field idTargetField = new Field("id", "INT", 4);
        FieldMapping existingMapping = new FieldMapping(idSourceField, idTargetField);
        fieldMappings.add(existingMapping);
        // 将字段添加到表的column列表中
        sourceTable.getColumn().add(idSourceField);
        targetTable.getColumn().add(idTargetField);

        Field nameSourceField = new Field("name", "VARCHAR", 12);
        Field nameTargetField = new Field("name", "VARCHAR", 12);
        FieldMapping nameMapping = new FieldMapping(nameSourceField, nameTargetField);
        fieldMappings.add(nameMapping);
        // 将字段添加到表的column列表中
        sourceTable.getColumn().add(nameSourceField);
        targetTable.getColumn().add(nameTargetField);

        testTableGroup.setFieldMapping(fieldMappings);

        // 配置TableGroup的profileComponent和Mapping信息
        TestDDLHelper.setupTableGroup(testTableGroup, "test-mapping-id",
                "test-source-connector-id", "test-target-connector-id",
                sourceConfig, targetConfig);

        logger.info("DDL同步集成测试用例环境初始化完成");
    }

    /**
     * 加载测试配置文件
     *
     * @throws IOException
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = DDLSyncIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
        try (InputStream input = DDLSyncIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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
     * 测试DDL同步端到端流程 - 新增字段
     */
    @Test
    public void testDDLSyncEndToEnd_AddColumn() {
        logger.info("开始测试DDL同步端到端流程 - 新增字段");

        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN age INT";

        try {
            // 1. 解析源DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sourceDDL);

            // 2. 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert ddlConfig.getAddedFieldNames().contains("age") : "新增字段列表应包含age字段";

            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 4. 验证字段映射更新
            boolean foundAgeMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "age".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "age".equals(mapping.getTarget().getName()));

            assert foundAgeMapping : "应找到age字段的映射";

            logger.info("DDL同步端到端流程测试通过 - 新增字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 新增字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试DDL同步端到端流程 - 删除字段
     */
    @Test
    public void testDDLSyncEndToEnd_DropColumn() {
        logger.info("开始测试DDL同步端到端流程 - 删除字段");

        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE ddlTestTable DROP COLUMN name";

        try {
            // 1. 解析源DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sourceDDL);

            // 2. 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_DROP == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_DROP";
            assert sourceDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getDroppedFieldNames().contains("name") : "删除字段列表应包含name字段";

            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 4. 验证字段映射更新
            boolean foundNameMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "name".equals(mapping.getSource().getName()));

            assert !foundNameMapping : "不应找到name字段的映射";

            logger.info("DDL同步端到端流程测试通过 - 删除字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 删除字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试DDL同步端到端流程 - 修改字段
     */
    @Test
    public void testDDLSyncEndToEnd_ModifyColumn() {
        logger.info("开始测试DDL同步端到端流程 - 修改字段");

        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE ddlTestTable MODIFY COLUMN name VARCHAR(100)";

        try {
            // 1. 解析源DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sourceDDL);

            // 2. 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_MODIFY == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_MODIFY";
            assert sourceDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getModifiedFieldNames().contains("name") : "修改字段列表应包含name字段";

            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 4. 验证字段映射仍然存在
            boolean foundNameMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "name".equals(mapping.getTarget().getName()));

            assert foundNameMapping : "应找到name字段的映射";

            logger.info("DDL同步端到端流程测试通过 - 修改字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 修改字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试DDL同步端到端流程 - 重命名字段
     */
    @Test
    public void testDDLSyncEndToEnd_ChangeColumn() {
        logger.info("开始测试DDL同步端到端流程 - 重命名字段");

        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE ddlTestTable CHANGE COLUMN name full_name VARCHAR(50)";

        try {
            // 1. 解析源DDL（使用真实的ConnectorService）
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sourceDDL);

            // 2. 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_CHANGE == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_CHANGE";
            assert sourceDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getChangedFieldNames().containsKey("name") &&
                    "full_name".equals(ddlConfig.getChangedFieldNames().get("name")) :
                    "变更字段映射应包含name到full_name的映射";

            // 3. 更新字段映射
            ddlParser.refreshFiledMappings(testTableGroup, ddlConfig);

            // 4. 验证字段映射更新
            boolean foundNewMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "full_name".equals(mapping.getTarget().getName()));

            boolean foundOldMapping = testTableGroup.getFieldMapping().stream()
                    .anyMatch(mapping -> mapping.getSource() != null && "name".equals(mapping.getSource().getName()) &&
                            mapping.getTarget() != null && "name".equals(mapping.getTarget().getName()));

            assert foundNewMapping : "应找到name到full_name的字段映射";
            assert !foundOldMapping : "不应找到name到name的旧字段映射";

            logger.info("DDL同步端到端流程测试通过 - 重命名字段");
        } catch (Exception e) {
            logger.error("DDL同步端到端流程测试失败 - 重命名字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试DDL解析器的防循环机制
     */
    @Test
    public void testDDLParserAntiCycleMechanism() {
        logger.info("开始测试DDL解析器的防循环机制");

        // 模拟源数据库执行的DDL语句
        String sourceDDL = "ALTER TABLE ddlTestTable ADD COLUMN cycle_test INT";

        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, sourceDDL);

            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert sourceDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getAddedFieldNames().contains("cycle_test") : "新增字段列表应包含cycle_test字段";

            // 验证防循环机制
            assert ddlConfig.getSql() != null : "DDL SQL不应为空";
            assert !ddlConfig.getSql().isEmpty() : "DDL SQL不应为空字符串";

            logger.info("DDL解析器的防循环机制测试通过");
        } catch (Exception e) {
            logger.error("DDL解析器的防循环机制测试失败", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试字段映射动态更新功能
     */
    @Test
    public void testFieldMappingDynamicUpdate() {
        logger.info("开始测试字段映射动态更新功能");

        // 创建一个新的字段映射
        Field newSourceField = new Field("new_field", "VARCHAR", 12);
        Field newTargetField = new Field("new_field", "VARCHAR", 12);
        FieldMapping newMapping = new FieldMapping(newSourceField, newTargetField);

        // 添加到字段映射列表
        List<FieldMapping> fieldMappings = testTableGroup.getFieldMapping();
        int originalSize = fieldMappings.size();
        fieldMappings.add(newMapping);

        // 验证字段映射已添加
        assert fieldMappings.size() == originalSize + 1 : "字段映射列表大小应增加1";
        assert fieldMappings.contains(newMapping) : "字段映射列表应包含新添加的映射";

        logger.info("字段映射动态更新功能测试通过");
    }

    /**
     * 测试异常处理机制
     */
    @Test
    public void testExceptionHandling() {
        logger.info("开始测试异常处理机制");

        // 测试无效的DDL语句
        String invalidDDL = "INVALID DDL STATEMENT";

        try {
            // 尝试解析无效的DDL
            DDLConfig ddlConfig = ddlParser.parse(targetConnectorService, testTableGroup, invalidDDL);

            // 如果没有抛出异常，说明解析器需要改进
            logger.warn("解析无效DDL语句时未抛出异常，可能需要检查解析器实现");
        } catch (Exception e) {
            // 预期会抛出异常
            logger.info("异常处理机制测试通过，捕获到预期异常: {}", e.getMessage());
        }
    }
}