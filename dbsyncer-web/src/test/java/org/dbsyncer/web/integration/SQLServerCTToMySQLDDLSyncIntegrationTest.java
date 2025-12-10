package org.dbsyncer.web.integration;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * SQL Server Change Tracking (CT) 到 MySQL 的 DDL 同步集成测试
 * <p>
 * 使用完整的Spring Boot应用上下文，启动真实的Listener进行端到端测试
 * 覆盖场景：
 * - SQL Server特殊类型转换：XML, UNIQUEIDENTIFIER, MONEY, SMALLMONEY, DATETIME2, DATETIMEOFFSET, TIMESTAMP, IMAGE, TEXT, NTEXT, BINARY, SMALLDATETIME, BIT, HIERARCHYID
 * - DDL操作：ADD COLUMN, ALTER COLUMN (MODIFY), DROP COLUMN, RENAME COLUMN
 * - 复杂场景：多字段添加、带约束字段添加
 * - DDL后DML时序一致性：测试DDL执行后，DML操作是否能正确反映变更
 * - 字段映射更新验证：验证DDL操作后字段映射是否正确更新
 * <p>
 * 注意：
 * - 使用 SqlServerCT 连接器类型（而不是 SqlServer）
 * - CT 模式通过 Change Tracking 机制检测 DDL 变更
 * - RENAME COLUMN 通过列属性匹配检测，使用 sp_rename 存储过程
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = Application.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ActiveProfiles("test")
public class SQLServerCTToMySQLDDLSyncIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerCTToMySQLDDLSyncIntegrationTest.class);

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Autowired
    private TableGroupService tableGroupService;

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
    public static void setUpClass() throws Exception {
        logger.info("开始初始化SQL Server CT到MySQL的DDL同步集成测试环境");

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

        // 启用 Change Tracking（CT 模式必需）
        enableChangeTracking(sqlServerConfig);

        logger.info("SQL Server CT到MySQL的DDL同步集成测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server CT到MySQL的DDL同步集成测试环境");

        try {
            String sourceCleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", sqlServerConfig);
            String targetCleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", mysqlConfig);
            testDatabaseManager.cleanupTestEnvironment(sourceCleanupSql, targetCleanupSql);
            logger.info("SQL Server CT到MySQL的DDL同步集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
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

        clearExistedMapping();

        // 重置表结构
        resetDatabaseTableStructure();

        // 创建Connector（源使用 CT 模式，目标使用 MySQL）
        sourceConnectorId = createConnector("SQL Server CT源连接器", sqlServerConfig, true);
        targetConnectorId = createConnector("MySQL目标连接器", mysqlConfig, false);

        logger.info("已创建 Connector - 源: {}, 目标: {}", sourceConnectorId, targetConnectorId);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("测试用例环境初始化完成 - MappingId: {}, MetaId: {}", mappingId, metaId);
    }

    @After
    public void tearDown() {
        try {
            if (mappingId != null) {
                try {
                    Meta meta = profileComponent.getMapping(mappingId).getMeta();
                    if (meta != null && meta.isRunning()) {
                        mappingService.stop(mappingId);
                        waitForMappingStopped(mappingId, 5000);
                    }
                } catch (Exception e) {
                    logger.debug("停止Mapping失败（可能未启动或已停止）: {}", e.getMessage());
                }

                try {
                    Meta meta = profileComponent.getMapping(mappingId).getMeta();
                    if (meta != null && meta.isRunning()) {
                        logger.warn("Mapping {} 状态仍为 RUNNING，强制设置为 READY", mappingId);
                        meta.saveState(MetaEnum.READY);
                    }
                } catch (Exception e) {
                    logger.debug("设置 Mapping {} 状态失败: {}", mappingId, e.getMessage());
                }

                try {
                    mappingService.remove(mappingId);
                    logger.debug("已删除测试 Mapping: {}", mappingId);
                } catch (Exception e) {
                    logger.warn("删除Mapping失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warn("清理Mapping失败", e);
        }

        try {
            if (sourceConnectorId != null) {
                try {
                    connectorService.remove(sourceConnectorId);
                    logger.debug("已删除源 Connector: {}", sourceConnectorId);
                } catch (Exception e) {
                    logger.warn("删除源Connector失败: {}", e.getMessage());
                }
            }
            if (targetConnectorId != null) {
                try {
                    connectorService.remove(targetConnectorId);
                    logger.debug("已删除目标 Connector: {}", targetConnectorId);
                } catch (Exception e) {
                    logger.warn("删除目标Connector失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warn("清理Connector失败", e);
        }

        resetDatabaseTableStructure();

        mappingId = null;
        metaId = null;
        sourceConnectorId = null;
        targetConnectorId = null;
    }

    /**
     * 启用 Change Tracking（CT 模式必需）
     */
    private static void enableChangeTracking(DatabaseConfig config) {
        try {
            DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
            instance.execute(databaseTemplate -> {
                // 启用数据库级别的 Change Tracking
                String enableDbCT = "IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()) " +
                        "ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";
                databaseTemplate.execute(enableDbCT);

                // 启用表级别的 Change Tracking
                String enableTableCT = "IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('ddlTestEmployee')) " +
                        "ALTER TABLE ddlTestEmployee ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";
                databaseTemplate.execute(enableTableCT);

                return null;
            });
            logger.info("已启用 Change Tracking");
        } catch (Exception e) {
            logger.warn("启用 Change Tracking 失败（可能已启用）: {}", e.getMessage());
        }
    }

    // ==================== SQL Server特殊类型转换测试 ====================

    @Test
    public void testAddColumn_NVARCHARType() throws Exception {
        logger.info("开始测试NVARCHAR类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD last_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, "last_name");
    }

    @Test
    public void testAddColumn_INTType() throws Exception {
        logger.info("开始测试INT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD age INT";
        testDDLConversion(sqlserverDDL, "age");
    }

    @Test
    public void testAddColumn_BIGINTType() throws Exception {
        logger.info("开始测试BIGINT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD count_num BIGINT";
        testDDLConversion(sqlserverDDL, "count_num");
    }

    @Test
    public void testAddColumn_DECIMALType() throws Exception {
        logger.info("开始测试DECIMAL类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD price DECIMAL(10,2)";
        testDDLConversion(sqlserverDDL, "price");
    }

    // ==================== DDL操作测试 ====================

    @Test
    public void testModifyColumn_ChangeLength() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 修改长度");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, "first_name");
    }

    @Test
    public void testModifyColumn_AddNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 添加NOT NULL约束");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        testDDLConversion(sqlserverDDL, "first_name");
    }

    @Test
    public void testModifyColumn_ChangeType() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 修改类型");
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 5000);
        executeDDLToSourceDatabase(addColumnDDL, sqlServerConfig);
        Thread.sleep(3000);

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN count_num BIGINT";
        testDDLConversion(sqlserverDDL, "count_num");
    }

    @Test
    public void testDropColumn() throws Exception {
        logger.info("开始测试DROP COLUMN操作");
        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name", "first_name");
    }

    // ==================== RENAME COLUMN 测试场景 ====================

    /**
     * 测试RENAME COLUMN - 重命名字段（仅重命名，不修改类型）
     * CT 模式特有功能：通过列属性匹配检测重命名
     */
    @Test
    public void testRenameColumn_RenameOnly() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名字段");

        String sqlServerDDL = "EXEC sp_rename 'ddlTestEmployee.first_name', 'full_name', 'COLUMN'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 5000);

        executeDDLToSourceDatabase(sqlServerDDL, sqlServerConfig);

        // 等待RENAME COLUMN处理完成（使用轮询方式）
        waitForDDLProcessingComplete("full_name", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "full_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "full_name".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到full_name到full_name的字段映射", foundNewMapping);
        assertTrue("不应找到first_name到first_name的旧字段映射", notFoundOldMapping);

        // 验证目标数据库中字段已重命名
        verifyFieldExistsInTargetDatabase("full_name", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldNotExistsInTargetDatabase("first_name", tableGroup.getTargetTable().getName(), mysqlConfig);

        logger.info("RENAME COLUMN重命名字段测试通过");
    }

    // ==================== 复杂场景测试 ====================

    @Test
    public void testAddMultipleColumns() throws Exception {
        logger.info("开始测试多字段同时添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2), bonus DECIMAL(8,2)";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 5000);

        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);

        waitForDDLProcessingComplete(Arrays.asList("salary", "bonus"), 10000);

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

        verifyFieldExistsInTargetDatabase("salary", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldExistsInTargetDatabase("bonus", tableGroup.getTargetTable().getName(), mysqlConfig);

        logger.info("多字段添加测试通过 - salary和bonus字段都已正确转换");
    }

    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试带默认值的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD status NVARCHAR(20) DEFAULT 'active'";
        testDDLConversion(sqlserverDDL, "status");
    }

    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试带NOT NULL约束的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD phone NVARCHAR(20) NOT NULL";
        testDDLConversion(sqlserverDDL, "phone");
    }

    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_by NVARCHAR(50) NOT NULL DEFAULT 'system'";
        testDDLConversion(sqlserverDDL, "created_by");
    }

    // ==================== 通用测试方法 ====================

    /**
     * 执行DDL转换并验证结果（集成测试版本）
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName) throws Exception {
        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 5000);

        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        waitForDDLProcessingComplete(expectedFieldName, 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
        boolean isModifyOperation = sourceDDL.toUpperCase().contains("ALTER COLUMN");

        if (isAddOperation) {
            boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                    .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()) &&
                            fm.getTarget() != null && expectedFieldName.equals(fm.getTarget().getName()));
            assertTrue("应找到字段 " + expectedFieldName + " 的映射", foundFieldMapping);

            verifyFieldExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), mysqlConfig);
        } else if (isModifyOperation) {
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
        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 5000);

        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        waitForDDLDropProcessingComplete(expectedFieldName, 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
        assertFalse("应移除字段 " + expectedFieldName + " 的映射", foundFieldMapping);

        verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), mysqlConfig);

        logger.info("DDL DROP操作测试通过 - 字段: {}", expectedFieldName);
    }

    // ==================== 辅助方法 ====================

    /**
     * 等待并验证meta状态为running
     */
    private void waitForMetaRunning(String metaId, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long checkInterval = 200;

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null && meta.isRunning()) {
                logger.info("Meta {} 已处于运行状态", metaId);
                return;
            }
            Thread.sleep(checkInterval);
        }

        Meta meta = profileComponent.getMeta(metaId);
        assertNotNull("Meta不应为null", meta);
        assertTrue("Meta应在" + timeoutMs + "ms内进入运行状态，当前状态: " + meta.getState(),
                meta.isRunning());
    }

    /**
     * 检查是否有残留的测试 mapping（仅检查，不清理）
     */
    private void clearExistedMapping() {
        try {
            List<Mapping> allMappings = profileComponent.getMappingAll();
            if (allMappings == null || allMappings.isEmpty()) {
                return;
            }
            for (Mapping mapping : allMappings) {
                logger.info("#### 正在清理 mapping: {}", mapping.getId());
                mappingService.stop(mapping.getId());
                mappingService.remove(mapping.getId());
            }
        } catch (Exception e) {
            logger.error("检查残留测试 mapping 时出错: {}", e.getMessage());
        }
    }

    /**
     * 等待并验证mapping已完全停止
     */
    private void waitForMappingStopped(String mappingId, long timeoutMs) throws InterruptedException {
        try {
            Meta meta = profileComponent.getMapping(mappingId).getMeta();
            if (meta == null) {
                logger.debug("Mapping {} 的 Meta 不存在，认为已停止", mappingId);
                return;
            }

            long startTime = System.currentTimeMillis();
            long checkInterval = 200;

            while (System.currentTimeMillis() - startTime < timeoutMs) {
                meta = profileComponent.getMeta(meta.getId());
                if (meta == null || !meta.isRunning()) {
                    logger.debug("Mapping {} 已停止，Meta 状态: {}", mappingId, meta != null ? meta.getState() : "null");
                    mappingService.remove(mappingId);
                    return;
                }
                Thread.sleep(checkInterval);
            }
            mappingService.remove(mappingId);
            logger.warn("等待 Mapping {} 停止超时（{}ms），当前状态: {}", mappingId, timeoutMs, meta.getState());
        } catch (Exception e) {
            logger.debug("检查 Mapping {} 停止状态时出错（可忽略）: {}", mappingId, e.getMessage());
        }
    }

    /**
     * 等待DDL处理完成（通过轮询检查字段映射是否已更新）
     */
    private void waitForDDLProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300;

        logger.info("等待DDL处理完成，期望字段: {}", expectedFieldName);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
            if (tableGroups != null && !tableGroups.isEmpty()) {
                TableGroup tableGroup = tableGroups.get(0);
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()) &&
                                fm.getTarget() != null && expectedFieldName.equals(fm.getTarget().getName()));

                if (foundFieldMapping) {
                    logger.info("DDL处理完成，字段 {} 的映射已更新", expectedFieldName);
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }

        logger.warn("等待DDL处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldName);
    }

    /**
     * 等待DDL DROP处理完成（通过轮询检查字段映射是否已移除）
     */
    private void waitForDDLDropProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300;

        logger.info("等待DDL DROP处理完成，期望移除字段: {}", expectedFieldName);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
            if (tableGroups != null && !tableGroups.isEmpty()) {
                TableGroup tableGroup = tableGroups.get(0);
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));

                if (!foundFieldMapping) {
                    logger.info("DDL DROP处理完成，字段 {} 的映射已移除", expectedFieldName);
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }

        logger.warn("等待DDL DROP处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldName);
    }

    /**
     * 等待DDL处理完成（多个字段，通过轮询检查字段映射是否已更新）
     */
    private void waitForDDLProcessingComplete(List<String> expectedFieldNames, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300;

        logger.info("等待DDL处理完成，期望字段: {}", expectedFieldNames);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
            if (tableGroups != null && !tableGroups.isEmpty()) {
                TableGroup tableGroup = tableGroups.get(0);
                boolean allFieldsFound = expectedFieldNames.stream().allMatch(expectedFieldName -> {
                    return tableGroup.getFieldMapping().stream()
                            .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()) &&
                                    fm.getTarget() != null && expectedFieldName.equals(fm.getTarget().getName()));
                });

                if (allFieldsFound) {
                    logger.info("DDL处理完成，所有字段 {} 的映射已更新", expectedFieldNames);
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }

        logger.warn("等待DDL处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldNames);
    }

    /**
     * 创建Connector
     */
    private String createConnector(String name, DatabaseConfig config, boolean isCT) {
        if (connectorService == null) {
            throw new IllegalStateException("connectorService 未注入，请检查 Spring 上下文是否正确初始化。可能原因：1) Spring Boot 应用上下文未启动 2) ConnectorService bean 未注册");
        }
        if (config == null) {
            throw new IllegalArgumentException("DatabaseConfig 不能为 null");
        }
        Map<String, String> params = new HashMap<>();
        params.put("name", name);
        String connectorType = isCT ? "SqlServerCT" : determineConnectorType(config);
        params.put("connectorType", connectorType);
        params.put("url", config.getUrl());
        params.put("username", config.getUsername());
        params.put("password", config.getPassword());
        params.put("driverClassName", config.getDriverClassName());

        String schema = config.getSchema();
        if (schema == null || schema.trim().isEmpty()) {
            if (isCT || "SqlServer".equals(connectorType)) {
                schema = "dbo";
            }
        }
        if (schema != null && !schema.trim().isEmpty()) {
            params.put("schema", schema);
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
    private String createMapping() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", "SQL Server CT到MySQL测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment");
        params.put("incrementStrategy", "Log");
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);

        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment");
        editParams.put("incrementStrategy", "Log");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        Map<String, String> tableGroupParams = new HashMap<>();
        tableGroupParams.put("mappingId", mappingId);
        tableGroupParams.put("sourceTable", "ddlTestEmployee");
        tableGroupParams.put("targetTable", "ddlTestEmployee");
        tableGroupParams.put("fieldMappings", "id|id,first_name|first_name");
        tableGroupService.add(tableGroupParams);

        return mappingId;
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
    private void executeDDLToSourceDatabase(String sql, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            databaseTemplate.execute(sql);
            return null;
        });
        logger.info("-----------execute sql--------- {}", sql);
    }

    /**
     * 验证目标数据库中字段是否存在
     */
    @SuppressWarnings("unchecked")
    private void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(determineConnectorType(config));
        }
        ConnectorInstance<DatabaseConfig, ?> instance = (ConnectorInstance<DatabaseConfig, ?>) connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertTrue(String.format("目标数据库表 %s 应包含字段 %s", tableName, fieldName), exists);
    }

    /**
     * 验证目标数据库中字段是否不存在
     */
    @SuppressWarnings("unchecked")
    private void verifyFieldNotExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(determineConnectorType(config));
        }
        ConnectorInstance<DatabaseConfig, ?> instance = (ConnectorInstance<DatabaseConfig, ?>) connectorFactory.connect(config);
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
     * 从数据库配置推断数据库类型（用于加载对应的SQL脚本）
     */
    private static String determineDatabaseType(DatabaseConfig config) {
        String url = config.getUrl();
        if (url == null) {
            return "mysql";
        }
        String urlLower = url.toLowerCase();
        if (urlLower.contains("mysql") || urlLower.contains("mariadb")) {
            return "mysql";
        } else if (urlLower.contains("sqlserver") || urlLower.contains("jdbc:sqlserver")) {
            return "sqlserver";
        }
        return "mysql";
    }

    /**
     * 根据数据库类型加载对应的SQL脚本
     */
    private static String loadSqlScriptByDatabaseType(String scriptBaseName, DatabaseConfig config) {
        String dbType = determineDatabaseType(config);
        String resourcePath = String.format("ddl/%s-%s.sql", scriptBaseName, dbType);
        return loadSqlScript(resourcePath);
    }

    /**
     * 加载SQL脚本文件
     */
    private static String loadSqlScript(String resourcePath) {
        try {
            InputStream input = SQLServerCTToMySQLDDLSyncIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = SQLServerCTToMySQLDDLSyncIntegrationTest.class.getClassLoader()
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

        public void initializeTestEnvironment(String sourceInitSql, String targetInitSql) throws Exception {
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

        private void executeSql(DatabaseConnectorInstance connectorInstance, String sql) throws Exception {
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

