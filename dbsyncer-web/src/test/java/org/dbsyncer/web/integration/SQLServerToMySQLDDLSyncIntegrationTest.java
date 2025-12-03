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
import org.springframework.context.annotation.Import;
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
 * SQL Server到MySQL的DDL同步集成测试
 * <p>
 * 使用完整的Spring Boot应用上下文，启动真实的Listener进行端到端测试
 * 覆盖场景：
 * - SQL Server特殊类型转换：XML, UNIQUEIDENTIFIER, MONEY, SMALLMONEY, DATETIME2, DATETIMEOFFSET, TIMESTAMP, IMAGE, TEXT, NTEXT, BINARY, SMALLDATETIME, BIT, HIERARCHYID
 * - DDL操作：ADD COLUMN, ALTER COLUMN (MODIFY), DROP COLUMN
 * - 复杂场景：多字段添加、带约束字段添加
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
            // 清理测试环境（使用按数据库类型分类的脚本）
            // 源数据库是SQL Server，目标数据库是MySQL，需要分别加载对应的清理脚本
            String sourceCleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", sqlServerConfig);
            String targetCleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", mysqlConfig);
            testDatabaseManager.cleanupTestEnvironment(sourceCleanupSql, targetCleanupSql);
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

        // 注意：不再在 setUp() 中清理残留配置，因为 tearDown() 应该确保完全清理
        // 如果前一个测试的 tearDown() 没有执行（如测试被强制终止），PreloadTemplate.launch() 可能会自动恢复残留的 mapping
        // 作为安全网，如果发现残留的测试 mapping，记录警告但不自动清理（让测试失败，便于发现问题）
        clearExistedMapping();

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
        // 停止并清理Mapping（必须先停止并删除Mapping，才能删除Connector）
        // 关键：确保测试完成后完全清理，防止 PreloadTemplate.launch() 在下一次测试启动时自动恢复
        try {
            if (mappingId != null) {
                // 1. 先停止Mapping（如果正在运行）
                try {
                    Meta meta = profileComponent.getMapping(mappingId).getMeta();
                    if (meta != null && meta.isRunning()) {
                        mappingService.stop(mappingId);
                        // 等待停止完成，确保 worker 线程完全停止
                        // IncrementPuller 会创建异步 worker 线程，需要等待其完全停止
                        waitForMappingStopped(mappingId, 5000);
                    }
                } catch (Exception e) {
                    logger.debug("停止Mapping失败（可能未启动或已停止）: {}", e.getMessage());
                }
                
                // 2. 强制将 meta 状态设置为非 RUNNING，防止 PreloadTemplate 自动恢复
                // 即使 stop() 失败，也要确保状态不是 RUNNING
                try {
                    Meta meta = profileComponent.getMapping(mappingId).getMeta();
                    if (meta != null && meta.isRunning()) {
                        logger.warn("Mapping {} 状态仍为 RUNNING，强制设置为 READY", mappingId);
                        meta.saveState(MetaEnum.READY);
                    }
                } catch (Exception e) {
                    logger.debug("设置 Mapping {} 状态失败: {}", mappingId, e.getMessage());
                }
                
                // 3. 删除Mapping（即使 stop() 失败也要删除，避免残留）
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

        // 清理Connector（必须在Mapping删除后）
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

        // 重置表结构
        resetDatabaseTableStructure();
        
        // 清空引用，防止重复清理
        mappingId = null;
        metaId = null;
        sourceConnectorId = null;
        targetConnectorId = null;
    }

    // ==================== SQL Server特殊类型转换测试 ====================

    @Test
    public void testAddColumn_XMLType() throws Exception {
        logger.info("开始测试XML类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD xml_data XML";
        testDDLConversion(sqlserverDDL, "xml_data");
    }

    @Test
    public void testAddColumn_UNIQUEIDENTIFIERType() throws Exception {
        logger.info("开始测试UNIQUEIDENTIFIER类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD guid UNIQUEIDENTIFIER";
        testDDLConversion(sqlserverDDL, "guid");
    }

    @Test
    public void testAddColumn_MONEYType() throws Exception {
        logger.info("开始测试MONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary MONEY";
        testDDLConversion(sqlserverDDL, "salary");
    }

    @Test
    public void testAddColumn_SMALLMONEYType() throws Exception {
        logger.info("开始测试SMALLMONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD bonus SMALLMONEY";
        testDDLConversion(sqlserverDDL, "bonus");
    }

    @Test
    public void testAddColumn_DATETIME2Type() throws Exception {
        logger.info("开始测试DATETIME2类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_at DATETIME2";
        testDDLConversion(sqlserverDDL, "created_at");
    }

    @Test
    public void testAddColumn_DATETIMEOFFSETType() throws Exception {
        logger.info("开始测试DATETIMEOFFSET类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIMEOFFSET";
        testDDLConversion(sqlserverDDL, "updated_at");
    }

    @Test
    public void testAddColumn_TIMESTAMPType() throws Exception {
        logger.info("开始测试TIMESTAMP类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD row_version TIMESTAMP";
        testDDLConversion(sqlserverDDL, "row_version");
    }

    @Test
    public void testAddColumn_IMAGEType() throws Exception {
        logger.info("开始测试IMAGE类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD photo IMAGE";
        testDDLConversion(sqlserverDDL, "photo");
    }

    @Test
    public void testAddColumn_TEXTType() throws Exception {
        logger.info("开始测试TEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD description TEXT";
        testDDLConversion(sqlserverDDL, "description");
    }

    @Test
    public void testAddColumn_NTEXTType() throws Exception {
        logger.info("开始测试NTEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD notes NTEXT";
        testDDLConversion(sqlserverDDL, "notes");
    }

    @Test
    public void testAddColumn_BINARYType() throws Exception {
        logger.info("开始测试BINARY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD binary_data BINARY(16)";
        testDDLConversion(sqlserverDDL, "binary_data");
    }

    @Test
    public void testAddColumn_VARBINARYMAXType() throws Exception {
        logger.info("开始测试VARBINARY(MAX)类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD varbinary_data VARBINARY(MAX)";
        testDDLConversion(sqlserverDDL, "varbinary_data");
    }

    @Test
    public void testAddColumn_SMALLDATETIMEType() throws Exception {
        logger.info("开始测试SMALLDATETIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD small_date SMALLDATETIME";
        testDDLConversion(sqlserverDDL, "small_date");
    }

    @Test
    public void testAddColumn_BITType() throws Exception {
        logger.info("开始测试BIT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD is_active BIT";
        testDDLConversion(sqlserverDDL, "is_active");
    }

    @Test
    public void testAddColumn_HIERARCHYIDType() throws Exception {
        logger.info("开始测试HIERARCHYID类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD org_path HIERARCHYID";
        testDDLConversion(sqlserverDDL, "org_path");
    }

    // ==================== SQL Server基础类型转换测试 ====================

    @Test
    public void testAddColumn_NVARCHARType() throws Exception {
        logger.info("开始测试NVARCHAR类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD last_name NVARCHAR(100)";
        testDDLConversion(sqlserverDDL, "last_name");
    }

    @Test
    public void testAddColumn_VARCHARType() throws Exception {
        logger.info("开始测试VARCHAR类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD code VARCHAR(50)";
        testDDLConversion(sqlserverDDL, "code");
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

    @Test
    public void testAddColumn_DATEType() throws Exception {
        logger.info("开始测试DATE类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD birth_date DATE";
        testDDLConversion(sqlserverDDL, "birth_date");
    }

    @Test
    public void testAddColumn_TIMEType() throws Exception {
        logger.info("开始测试TIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD work_time TIME";
        testDDLConversion(sqlserverDDL, "work_time");
    }

    @Test
    public void testAddColumn_DATETIMEType() throws Exception {
        logger.info("开始测试DATETIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIME";
        testDDLConversion(sqlserverDDL, "updated_at");
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
        // 先添加一个INT字段用于测试类型修改
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
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 5000);
        executeDDLToSourceDatabase(setNotNullDDL, sqlServerConfig);
        Thread.sleep(3000);

        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NULL";
        testDDLConversion(sqlserverDDL, "first_name");
    }

    @Test
    public void testDropColumn() throws Exception {
        logger.info("开始测试DROP COLUMN操作");
        testDDLDropOperation("ALTER TABLE ddlTestEmployee DROP COLUMN first_name", "first_name");
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
        
        // 等待DDL处理完成（多个字段）
        waitForDDLProcessingComplete(Arrays.asList("salary", "bonus"), 10000);

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
        // 启动Mapping
        logger.info("#### mapping service start for: {}", mappingId);
        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL
        waitForMetaRunning(metaId, 5000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);
        
        // 等待DDL处理完成（使用轮询方式，最多等待10秒）
        waitForDDLProcessingComplete(expectedFieldName, 10000);

        // 验证字段映射是否更新
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

            // 验证目标数据库中字段是否存在
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
        // 启动Mapping
        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL
        waitForMetaRunning(metaId, 5000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);
        
        // 等待DDL处理完成（字段被移除）
        waitForDDLDropProcessingComplete(expectedFieldName, 10000);

        // 验证字段映射是否已移除
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
        assertFalse("应移除字段 " + expectedFieldName + " 的映射", foundFieldMapping);

        // 验证目标数据库中字段是否已被删除
        verifyFieldNotExistsInTargetDatabase(expectedFieldName, tableGroup.getTargetTable().getName(), mysqlConfig);

        logger.info("DDL DROP操作测试通过 - 字段: {}", expectedFieldName);
    }

    // ==================== 辅助方法 ====================

    /**
     * 等待并验证meta状态为running
     * 
     * @param metaId meta的ID
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     * @throws AssertionError 如果超时后meta仍未运行
     */
    private void waitForMetaRunning(String metaId, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long checkInterval = 200; // 每200ms检查一次
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null && meta.isRunning()) {
                logger.info("Meta {} 已处于运行状态", metaId);
                return;
            }
            Thread.sleep(checkInterval);
        }
        
        // 超时后再次检查一次，如果仍未运行则抛出异常
        Meta meta = profileComponent.getMeta(metaId);
        assertNotNull("Meta不应为null", meta);
        assertTrue("Meta应在" + timeoutMs + "ms内进入运行状态，当前状态: " + meta.getState(), 
                   meta.isRunning());
    }

    /**
     * 检查是否有残留的测试 mapping（仅检查，不清理）
     * 作为安全网，如果发现残留的测试 mapping，记录警告但不自动清理
     * 这样可以让测试失败，便于发现 tearDown() 没有正确执行的问题
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
     * 清理所有可能残留的测试 mapping（已废弃，不再使用）
     * 现在依赖 tearDown() 确保完全清理
     * 
     * @deprecated 使用 tearDown() 确保测试后完全清理，不再需要此方法
     */
    @Deprecated
    private void cleanupOrphanedTestMappings() {
        try {
            List<Mapping> allMappings = profileComponent.getMappingAll();
            if (allMappings == null || allMappings.isEmpty()) {
                return;
            }

            int cleanedCount = 0;
            for (Mapping mapping : allMappings) {
                // 只清理测试相关的 mapping（名称包含"测试"或"Test"）
                String mappingName = mapping.getName();
                if (mappingName != null && (mappingName.contains("测试") || mappingName.contains("Test"))) {
                    try {
                        String mappingId = mapping.getId();
                        Meta meta = profileComponent.getMeta(mapping.getMetaId());
                        
                        // 如果 mapping 正在运行，先停止它
                        // 注意：PreloadTemplate.launch() 可能已经自动启动了残留的 mapping
                        if (meta != null && meta.isRunning()) {
                            logger.warn("发现残留的测试 mapping [{}] 正在运行（可能由 PreloadTemplate 自动启动），开始清理", mappingId);
                            try {
                                mappingService.stop(mappingId);
                                // 等待停止完成，确保 worker 线程完全停止
                                waitForMappingStopped(mappingId, 3000);
                            } catch (Exception e) {
                                logger.debug("停止残留 mapping {} 失败: {}", mappingId, e.getMessage());
                            }
                        }
                        
                        // 删除 mapping
                        try {
                            mappingService.remove(mappingId);
                            cleanedCount++;
                            logger.info("已清理残留的测试 mapping: {} ({})", mappingId, mappingName);
                        } catch (Exception e) {
                            logger.warn("删除残留 mapping {} 失败: {}", mappingId, e.getMessage());
                        }
                    } catch (Exception e) {
                        logger.warn("清理残留 mapping {} 时出错: {}", mapping.getId(), e.getMessage());
                    }
                }
            }
            
            if (cleanedCount > 0) {
                logger.info("清理完成，共清理了 {} 个残留的测试 mapping", cleanedCount);
            }
        } catch (Exception e) {
            logger.warn("清理残留测试 mapping 时出错: {}", e.getMessage());
        }
    }

    /**
     * 等待并验证mapping已完全停止
     * 
     * @param mappingId mapping的ID
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     */
    private void waitForMappingStopped(String mappingId, long timeoutMs) throws InterruptedException {
        try {
            Meta meta = profileComponent.getMapping(mappingId).getMeta();
            if (meta == null) {
                logger.debug("Mapping {} 的 Meta 不存在，认为已停止", mappingId);
                return;
            }
            
            long startTime = System.currentTimeMillis();
            long checkInterval = 200; // 每200ms检查一次
            
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                // 重新获取 meta 状态
                meta = profileComponent.getMeta(meta.getId());
                if (meta == null || !meta.isRunning()) {
                    logger.debug("Mapping {} 已停止，Meta 状态: {}", mappingId, meta != null ? meta.getState() : "null");
                    mappingService.remove(mappingId);
                    return;
                }
                Thread.sleep(checkInterval);
            }
            mappingService.remove(mappingId);
            // 超时后记录警告
            logger.warn("等待 Mapping {} 停止超时（{}ms），当前状态: {}", mappingId, timeoutMs, meta.getState());
        } catch (Exception e) {
            logger.debug("检查 Mapping {} 停止状态时出错（可忽略）: {}", mappingId, e.getMessage());
        }
    }

    /**
     * 等待DDL处理完成（通过轮询检查字段映射是否已更新）
     * 
     * @param expectedFieldName 期望的字段名
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     * @throws Exception 如果查询TableGroup时发生异常
     */
    private void waitForDDLProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300; // 每300ms检查一次
        
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
                    // 额外等待一小段时间，确保目标数据库的DDL也已执行完成
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }
        
        // 超时后记录警告，但不抛出异常（让后续的断言来处理）
        logger.warn("等待DDL处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldName);
    }

    /**
     * 等待DDL DROP处理完成（通过轮询检查字段映射是否已移除）
     * 
     * @param expectedFieldName 期望被移除的字段名
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     * @throws Exception 如果查询TableGroup时发生异常
     */
    private void waitForDDLDropProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300; // 每300ms检查一次
        
        logger.info("等待DDL DROP处理完成，期望移除字段: {}", expectedFieldName);
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
            if (tableGroups != null && !tableGroups.isEmpty()) {
                TableGroup tableGroup = tableGroups.get(0);
                boolean foundFieldMapping = tableGroup.getFieldMapping().stream()
                        .anyMatch(fm -> fm.getSource() != null && expectedFieldName.equals(fm.getSource().getName()));
                
                if (!foundFieldMapping) {
                    logger.info("DDL DROP处理完成，字段 {} 的映射已移除", expectedFieldName);
                    // 额外等待一小段时间，确保目标数据库的DDL也已执行完成
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }
        
        // 超时后记录警告，但不抛出异常（让后续的断言来处理）
        logger.warn("等待DDL DROP处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldName);
    }

    /**
     * 等待DDL处理完成（多个字段，通过轮询检查字段映射是否已更新）
     * 
     * @param expectedFieldNames 期望的字段名列表
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     * @throws Exception 如果查询TableGroup时发生异常
     */
    private void waitForDDLProcessingComplete(List<String> expectedFieldNames, long timeoutMs) throws InterruptedException, Exception {
        long startTime = System.currentTimeMillis();
        long checkInterval = 300; // 每300ms检查一次
        
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
                    // 额外等待一小段时间，确保目标数据库的DDL也已执行完成
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }
        
        // 超时后记录警告，但不抛出异常（让后续的断言来处理）
        logger.warn("等待DDL处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldNames);
    }

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
        String connectorType = determineConnectorType(config);
        params.put("connectorType", connectorType);
        params.put("url", config.getUrl());
        params.put("username", config.getUsername());
        params.put("password", config.getPassword());
        params.put("driverClassName", config.getDriverClassName());
        
        // 设置 schema：SQL Server 需要 schema（默认为 dbo），MySQL 可以为空
        String schema = config.getSchema();
        if (schema == null || schema.trim().isEmpty()) {
            if ("SqlServer".equals(connectorType)) {
                schema = "dbo";  // SQL Server 默认 schema
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
        params.put("name", "SQL Server到MySQL测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment"); // 增量同步（使用 "increment" 而不是 "1"）
        params.put("incrementStrategy", "Log"); // 增量策略：日志监听（SQL Server 使用日志监听）
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        // 创建Mapping（不传入 tableGroups，稍后单独创建 TableGroup）
        String mappingId = mappingService.add(params);
        
        // 创建后需要编辑一次以正确设置增量同步配置（因为 checkAddConfigModel 默认是全量同步）
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment"); // 使用 "increment" 而不是 "1"
        editParams.put("incrementStrategy", "Log");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);
        
        // 直接使用 tableGroupService.add() 创建 TableGroup
        // 格式：id|id,first_name|first_name
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
    private void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置，否则 ConnectorFactory 无法找到对应的 ConnectorService
        if (config.getConnectorType() == null) {
            config.setConnectorType(determineConnectorType(config));
        }
        ConnectorInstance<DatabaseConfig, ?> instance = connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertTrue(String.format("目标数据库表 %s 应包含字段 %s", tableName, fieldName), exists);
    }

    /**
     * 验证目标数据库中字段是否不存在
     */
    private void verifyFieldNotExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置，否则 ConnectorFactory 无法找到对应的 ConnectorService
        if (config.getConnectorType() == null) {
            config.setConnectorType(determineConnectorType(config));
        }
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
     * 
     * @param scriptBaseName 脚本基础名称（不包含数据库类型后缀和扩展名）
     * @param config 数据库配置，用于推断数据库类型
     * @return SQL脚本内容
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
            InputStream input = SQLServerToMySQLDDLSyncIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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

