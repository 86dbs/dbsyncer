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
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

/**
 * SQL Server到MySQL的DDL同步集成测试
 * <p>
 * 使用完整的Spring Boot应用上下文，启动真实的Listener进行端到端测试
 * 覆盖场景：
 * - sp_rename列重命名：测试SQL Server特有的列重命名操作是否能自动检测并同步到MySQL
 * - DDL后DML时序一致性：测试DDL执行后，DML操作是否能正确反映变更
 * - 字段映射更新验证：验证DDL操作后字段映射是否正确更新
 * <p>
 * 注意：
 * - ConnectorService 是用来管理连接器配置的服务，它本身不区分源和目标数据库
 * - 通过 connectorService.add() 创建不同的 Connector 实例来区分源和目标
 * - 每个 Connector 实例包含独立的数据库连接配置（URL、用户名、密码等）
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = Application.class,  // 使用完整的 Application，包含所有组件
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT  // 使用完整 Web 环境，自动配置所有 Actuator 端点
)
@ActiveProfiles("test")
@Import(TestActuatorConfiguration.class)  // 导入测试配置，禁用 Spring Security
public class SQLServerToMySQLDDLSyncIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerToMySQLDDLSyncIntegrationTest.class);

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Autowired
    private ProfileComponent profileComponent;

    @Autowired
    private ConnectorFactory connectorFactory;

    private static DatabaseConfig sqlServerConfig;
    private static DatabaseConfig mysqlConfig;
    private static TestDatabaseManager testDatabaseManager;

    private String sourceConnectorId;
    private String targetConnectorId;
    private String mappingId;
    private String metaId;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化SQL Server到MySQL的DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sqlServerConfig, mysqlConfig);

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

        logger.info("SQL Server到MySQL的DDL同步集成测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server到MySQL的DDL同步集成测试环境");

        try {
            String cleanupSql = "DROP TABLE IF EXISTS ddlTestEmployee;";
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);
            logger.info("SQL Server到MySQL的DDL同步集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 确保依赖注入已完成
        // 注意：如果这些服务为 null，说明 Spring 上下文没有正确初始化
        // 可能的原因：
        // 1. Spring Boot 应用上下文启动失败
        // 2. 组件扫描路径不正确
        // 3. Bean 注册失败
        if (connectorService == null) {
            throw new IllegalStateException(
                    "connectorService 未注入，请检查 Spring 上下文是否正确初始化。\n" +
                            "可能原因：\n" +
                            "1. Spring Boot 应用上下文启动失败，请检查日志\n" +
                            "2. ConnectorService bean 未注册（应在 org.dbsyncer.biz.impl.ConnectorServiceImpl）\n" +
                            "3. 组件扫描路径不正确（Application 类应配置 scanBasePackages = \"org.dbsyncer\"）"
            );
        }
        if (mappingService == null) {
            throw new IllegalStateException("mappingService 未注入，请检查 Spring 上下文是否正确初始化");
        }
        if (profileComponent == null) {
            throw new IllegalStateException("profileComponent 未注入，请检查 Spring 上下文是否正确初始化");
        }
        if (connectorFactory == null) {
            throw new IllegalStateException("connectorFactory 未注入，请检查 Spring 上下文是否正确初始化");
        }

        logger.info("Spring 依赖注入验证通过 - connectorService: {}, mappingService: {}, profileComponent: {}, connectorFactory: {}",
                connectorService != null, mappingService != null, profileComponent != null, connectorFactory != null);

        // 重置表结构
        resetDatabaseTableStructure();

        // 创建Connector
        // 注意：connectorService 本身不区分源和目标，通过创建不同的 Connector 实例来区分
        // 每个 Connector 包含独立的数据库连接配置（URL、用户名、密码等）
        sourceConnectorId = createConnector("SQL Server源连接器", sqlServerConfig);
        targetConnectorId = createConnector("MySQL目标连接器", mysqlConfig);

        logger.info("已创建 Connector - 源: {}, 目标: {}", sourceConnectorId, targetConnectorId);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("测试用例环境初始化完成 - MappingId: {}, MetaId: {}", mappingId, metaId);
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

    @Test
    public void testSpRenameColumn_EndToEnd() throws Exception {
        logger.info("开始测试 sp_rename 列重命名的端到端集成测试");

        // 1. 添加字段 old_name
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD old_name NVARCHAR(50)";
        executeDDLToSourceDatabase(addColumnDDL, sqlServerConfig);

        // 等待DDL同步（在实际场景中，DDL事件会被自动捕获）
        Thread.sleep(2000);

        // 2. 启动Mapping（启动Listener）
        mappingService.start(mappingId);
        logger.info("已启动Mapping，Listener应已开始运行");

        // 等待Listener启动
        Thread.sleep(3000);

        // 3. 执行 sp_rename 重命名列
        String spRenameSQL = "EXEC sp_rename 'ddlTestEmployee.old_name', 'new_name', 'COLUMN'";
        executeDDLToSourceDatabase(spRenameSQL, sqlServerConfig);
        logger.info("已执行 sp_rename，列名从 old_name 重命名为 new_name");

        // 4. 插入DML数据触发检测（在真实场景中，SqlServerListener.pull()会检测到列名不一致）
        String insertSQL = "INSERT INTO ddlTestEmployee (first_name, new_name) VALUES ('TestUser', 'test@example.com')";
        executeDDLToSourceDatabase(insertSQL, sqlServerConfig);
        logger.info("已插入DML数据，应触发 sp_rename 检测逻辑");

        // 5. 等待检测和处理完成（Listener会检测到列名不一致，生成DDL事件并处理）
        Thread.sleep(5000);

        // 6. 验证目标数据库字段名已更新（集成测试关注最终结果）
        verifyFieldNotExistsInTargetDatabase("old_name", "ddlTestEmployee", mysqlConfig);
        verifyFieldExistsInTargetDatabase("new_name", "ddlTestEmployee", mysqlConfig);

        // 7. 验证字段映射已更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean oldNameMappingExists = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "old_name".equals(fm.getSource().getName()));
        assertFalse("应移除旧字段 old_name 的映射", oldNameMappingExists);

        boolean newNameMappingExists = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "new_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "new_name".equals(fm.getTarget().getName()));
        assertTrue("应找到新字段 new_name 的映射", newNameMappingExists);

        logger.info("sp_rename 列重命名端到端集成测试完成：目标库字段名已更新，字段映射已更新");
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Connector
     */
    private String createConnector(String name, DatabaseConfig config) {
        if (connectorService == null) {
            throw new IllegalStateException("connectorService 未注入，请检查 Spring 上下文是否正确初始化。可能原因：1) Spring Boot 应用上下文未启动 2) ConnectorService bean 未注册");
        }
        if (config == null) {
            throw new IllegalArgumentException("DatabaseConfig 不能为 null");
        }
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
        try {
            return connectorService.add(params);
        } catch (Exception e) {
            logger.error("创建 Connector 失败 - name: {}, url: {}", name, config.getUrl(), e);
            throw new RuntimeException("创建 Connector 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 创建Mapping和TableGroup
     */
    private String createMapping() {
        Map<String, String> params = new HashMap<>();
        params.put("name", "SQL Server到MySQL测试Mapping");
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

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("source", "first_name");
        nameMapping.put("target", "first_name");
        fieldMappings.add(nameMapping);

        tableGroup.put("fieldMapping", fieldMappings);

        List<Map<String, Object>> tableGroups = new ArrayList<>();
        tableGroups.add(tableGroup);

        params.put("tableGroups", org.dbsyncer.common.util.JsonUtil.objToJson(tableGroups));

        return mappingService.add(params);
    }

    /**
     * 重置数据库表结构
     */
    private void resetDatabaseTableStructure() {
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
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
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
     * 加载测试配置
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = SQLServerToMySQLDDLSyncIntegrationTest.class.getClassLoader()
                .getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                sqlServerConfig = createDefaultSQLServerConfig();
                mysqlConfig = createDefaultMySQLConfig();
                return;
            }
            props.load(input);
        }

        sqlServerConfig = new DatabaseConfig();
        sqlServerConfig.setUrl(props.getProperty("test.db.sqlserver.url",
                "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true"));
        sqlServerConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sqlServerConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sqlServerConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver"));

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
     * 测试数据库管理器
     */
    private static class TestDatabaseManager {
        private final DatabaseConnectorInstance sourceConnectorInstance;
        private final DatabaseConnectorInstance targetConnectorInstance;

        public TestDatabaseManager(DatabaseConfig sourceConfig, DatabaseConfig targetConfig) {
            this.sourceConnectorInstance = new DatabaseConnectorInstance(sourceConfig);
            this.targetConnectorInstance = new DatabaseConnectorInstance(targetConfig);
        }

        public void initializeTestEnvironment(String sourceInitSql, String targetInitSql) {
            executeSql(sourceConnectorInstance, sourceInitSql);
            executeSql(targetConnectorInstance, targetInitSql);
        }

        public void cleanupTestEnvironment(String sourceCleanupSql, String targetCleanupSql) {
            try {
                executeSql(sourceConnectorInstance, sourceCleanupSql);
                executeSql(targetConnectorInstance, targetCleanupSql);
            } catch (Exception e) {
                logger.warn("清理测试环境失败", e);
            }
        }

        public void resetTableStructure(String sourceResetSql, String targetResetSql) {
            try {
                executeSql(sourceConnectorInstance, sourceResetSql);
                executeSql(targetConnectorInstance, targetResetSql);
            } catch (Exception e) {
                logger.warn("重置表结构失败", e);
            }
        }

        private void executeSql(DatabaseConnectorInstance connectorInstance, String sql) {
            if (sql == null || sql.trim().isEmpty()) {
                return;
            }
            connectorInstance.execute(databaseTemplate -> {
                String[] sqlStatements = sql.split(";");
                for (String sqlStatement : sqlStatements) {
                    String trimmedSql = sqlStatement.trim();
                    if (!trimmedSql.isEmpty()) {
                        try {
                            databaseTemplate.execute(trimmedSql);
                        } catch (Exception e) {
                            logger.debug("SQL执行失败（可能可忽略）: {}", trimmedSql, e);
                        }
                    }
                }
                return null;
            });
        }
    }
}

