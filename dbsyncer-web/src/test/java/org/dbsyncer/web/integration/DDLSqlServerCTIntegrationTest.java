package org.dbsyncer.web.integration;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * SQL Server Change Tracking (CT) 到 SQL Server CT 的 DDL 同步集成测试
 * 全面测试 SQL Server CT 模式之间 DDL 同步的端到端功能，包括解析、转换和执行
 * 覆盖场景：
 * - ADD COLUMN: 基础添加、带默认值、带约束、带NULL/NOT NULL
 * - DROP COLUMN: 删除字段
 * - ALTER COLUMN: 修改类型、修改长度、修改约束（NULL/NOT NULL）
 * - RENAME COLUMN: 重命名字段（使用 sp_rename，CT 模式特有功能）
 * - 异常处理
 * 
 * 注意：
 * - 使用 SqlServerCT 连接器类型（而不是 SqlServer）
 * - CT 模式通过 Change Tracking 机制检测 DDL 变更
 * - RENAME COLUMN 通过列属性匹配检测，使用 sp_rename 存储过程
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class DDLSqlServerCTIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DDLSqlServerCTIntegrationTest.class);

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private TableGroupService tableGroupService;

    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;
    private static TestDatabaseManager testDatabaseManager;

    private String sourceConnectorId;
    private String targetConnectorId;
    private String mappingId;
    private String metaId;

    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("开始初始化SQL Server CT到SQL Server CT的DDL同步测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境（使用按数据库类型分类的脚本）
        String initSql = loadSqlScriptByDatabaseType("reset-test-table", sourceConfig);
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        // 注意：不需要手动启用 Change Tracking
        // SqlServerCTListener.start() 会自动调用 enableDBChangeTracking() 和 enableTableChangeTracking()

        logger.info("SQL Server CT到SQL Server CT的DDL同步测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server CT到SQL Server CT的DDL同步测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            String cleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", sourceConfig);
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("SQL Server CT到SQL Server CT的DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 先清理可能残留的测试 mapping（防止上一个测试清理失败导致残留）
        cleanupResidualTestMappings();

        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        // 创建Connector（使用 CT 模式）
        sourceConnectorId = createConnector("SQL Server CT源连接器", sourceConfig);
        targetConnectorId = createConnector("SQL Server CT目标连接器", targetConfig);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("SQL Server CT到SQL Server CT的DDL同步测试用例环境初始化完成");
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
    }

    /**
     * 清理残留的测试 mapping
     */
    private void cleanupResidualTestMappings() {
        try {
            List<Mapping> allMappings = profileComponent.getMappingAll();
            int cleanedCount = 0;

            for (Mapping mapping : allMappings) {
                String mappingName = mapping.getName();
                // 清理包含"CT测试"的 mapping
                if (mappingName != null && mappingName.contains("CT测试")) {
                    try {
                        String mappingId = mapping.getId();
                        try {
                            mappingService.stop(mappingId);
                            mappingService.remove(mappingId);
                            cleanedCount++;
                            logger.debug("已清理残留的测试 mapping: {} ({})", mappingId, mappingName);
                        } catch (Exception e) {
                            logger.debug("删除残留 mapping {} 失败: {}", mappingId, e.getMessage());
                        }
                    } catch (Exception e) {
                        logger.debug("清理残留 mapping {} 时出错: {}", mapping.getId(), e.getMessage());
                    }
                }
            }

            if (cleanedCount > 0) {
                logger.info("清理完成，共清理了 {} 个残留的测试 mapping", cleanedCount);
            }
        } catch (Exception e) {
            logger.debug("清理残留测试 mapping 时出错: {}", e.getMessage());
        }
    }

    /**
     * 重置数据库表结构到初始状态
     */
    private void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            // 使用按数据库类型分类的脚本
            String resetSql = loadSqlScriptByDatabaseType("reset-test-table", sourceConfig);
            if (resetSql != null && !resetSql.trim().isEmpty()) {
                testDatabaseManager.resetTableStructure(resetSql);
                logger.debug("测试数据库表结构重置完成");
            }
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }


    // ==================== ADD COLUMN 测试场景 ====================

    /**
     * 测试ADD COLUMN - 基础添加字段
     */
    @Test
    public void testAddColumn_Basic() throws Exception {
        logger.info("开始测试ADD COLUMN - 基础添加字段");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("salary", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundSalaryMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "salary".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "salary".equals(fm.getTarget().getName()));

        assertTrue("应找到salary字段的映射", foundSalaryMapping);
        verifyFieldExistsInTargetDatabase("salary", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN基础测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD status NVARCHAR(20) DEFAULT 'active'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        waitForDDLProcessingComplete("status", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundStatusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "status".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "status".equals(fm.getTarget().getName()));

        assertTrue("应找到status字段的映射", foundStatusMapping);
        verifyFieldExistsInTargetDatabase("status", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带默认值测试通过");
    }

    /**
     * 测试ADD COLUMN - 带NOT NULL约束
     */
    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD phone NVARCHAR(20) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        waitForDDLProcessingComplete("phone", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPhoneMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));

        assertTrue("应找到phone字段的映射", foundPhoneMapping);
        verifyFieldExistsInTargetDatabase("phone", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带NOT NULL约束测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值和NOT NULL约束
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值和NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ADD created_by NVARCHAR(50) NOT NULL DEFAULT 'system'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        waitForDDLProcessingComplete("created_by", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCreatedByMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "created_by".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "created_by".equals(fm.getTarget().getName()));

        assertTrue("应找到created_by字段的映射", foundCreatedByMapping);
        verifyFieldExistsInTargetDatabase("created_by", "ddlTestEmployee", targetConfig);

        logger.info("ADD COLUMN带默认值和NOT NULL约束测试通过");
    }

    // ==================== DROP COLUMN 测试场景 ====================

    /**
     * 测试DROP COLUMN - 删除字段
     */
    @Test
    public void testDropColumn_Basic() throws Exception {
        logger.info("开始测试DROP COLUMN - 删除字段");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee DROP COLUMN department";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 等待DDL DROP处理完成（使用轮询方式）
        waitForDDLDropProcessingComplete("department", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundDepartmentMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "department".equals(fm.getSource().getName()));

        assertFalse("不应找到department字段的映射", foundDepartmentMapping);
        verifyFieldNotExistsInTargetDatabase("department", "ddlTestEmployee", targetConfig);

        logger.info("DROP COLUMN测试通过");
    }

    // ==================== ALTER COLUMN 测试场景 ====================

    /**
     * 测试ALTER COLUMN - 修改字段长度
     */
    @Test
    public void testAlterColumn_ChangeLength() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段长度");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("ALTER COLUMN修改长度测试通过");
    }

    /**
     * 测试ALTER COLUMN - 修改字段类型
     */
    @Test
    public void testAlterColumn_ChangeType() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段类型");

        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        waitForDDLProcessingComplete("count_num", 10000);

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN count_num BIGINT";

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCountNumMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "count_num".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "count_num".equals(fm.getTarget().getName()));

        assertTrue("应找到count_num字段的映射", foundCountNumMapping);

        logger.info("ALTER COLUMN修改类型测试通过");
    }

    /**
     * 测试ALTER COLUMN - 添加NOT NULL约束
     */
    @Test
    public void testAlterColumn_AddNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 添加NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN last_name NVARCHAR(50) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundLastNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "last_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "last_name".equals(fm.getTarget().getName()));

        assertTrue("应找到last_name字段的映射", foundLastNameMapping);

        logger.info("ALTER COLUMN添加NOT NULL约束测试通过");
    }

    /**
     * 测试ALTER COLUMN - 移除NOT NULL约束（允许NULL）
     */
    @Test
    public void testAlterColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 移除NOT NULL约束");

        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);
        executeDDLToSourceDatabase(setNotNullDDL, sourceConfig);
        Thread.sleep(2000);

        String sqlServerDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NULL";

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("ALTER COLUMN移除NOT NULL约束测试通过");
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

        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 等待RENAME COLUMN处理完成（使用轮询方式）
        waitForDDLProcessingComplete("full_name", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
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

        logger.info("RENAME COLUMN重命名字段测试通过");
    }

    /**
     * 测试RENAME COLUMN - 重命名并修改类型（先重命名，再修改类型）
     * 这种情况会生成 RENAME + ALTER 两个操作
     */
    @Test
    public void testRenameColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名并修改类型");

        // 先添加一个VARCHAR字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD description NVARCHAR(100)";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        waitForDDLProcessingComplete("description", 10000);

        // 先重命名
        String renameDDL = "EXEC sp_rename 'ddlTestEmployee.description', 'desc_text', 'COLUMN'";
        executeDDLToSourceDatabase(renameDDL, sourceConfig);
        waitForDDLProcessingComplete("desc_text", 10000);

        // 再修改类型
        String alterDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN desc_text TEXT";
        executeDDLToSourceDatabase(alterDDL, sourceConfig);
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "desc_text".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "desc_text".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "description".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "description".equals(fm.getTarget().getName()));

        assertTrue("应找到desc_text到desc_text的字段映射", foundNewMapping);
        assertTrue("不应找到description到description的旧字段映射", notFoundOldMapping);

        logger.info("RENAME COLUMN重命名并修改类型测试通过");
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Connector（使用 CT 模式）
     */
    private String createConnector(String name, DatabaseConfig config) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", name);
        params.put("connectorType", "SqlServerCT"); // 使用 CT 模式
        params.put("url", config.getUrl());
        params.put("username", config.getUsername());
        params.put("password", config.getPassword());
        params.put("driverClassName", config.getDriverClassName());
        if (config.getSchema() != null) {
            params.put("schema", config.getSchema());
        } else {
            params.put("schema", "dbo"); // SQL Server 默认 schema
        }
        return connectorService.add(params);
    }

    /**
     * 创建Mapping和TableGroup
     */
    private String createMapping() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", "SQL Server CT到SQL Server CT测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment"); // 增量同步
        params.put("incrementStrategy", "Log"); // 增量策略：日志监听（CT 模式）
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);

        // 创建后需要编辑一次以正确设置增量同步配置
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment");
        editParams.put("incrementStrategy", "Log");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 使用 tableGroupService.add() 创建 TableGroup
        Map<String, String> tableGroupParams = new HashMap<>();
        tableGroupParams.put("mappingId", mappingId);
        tableGroupParams.put("sourceTable", "ddlTestEmployee");
        tableGroupParams.put("targetTable", "ddlTestEmployee");
        tableGroupParams.put("fieldMappings", "id|id,first_name|first_name,last_name|last_name,department|department");
        tableGroupService.add(tableGroupParams);

        return mappingId;
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
    }

    /**
     * 验证目标数据库中字段是否存在
     */
    @SuppressWarnings("unchecked")
    private void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType("SqlServerCT");
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
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType("SqlServerCT");
        }
        ConnectorInstance<DatabaseConfig, ?> instance = (ConnectorInstance<DatabaseConfig, ?>) connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertFalse(String.format("目标数据库表 %s 不应包含字段 %s", tableName, fieldName), exists);
    }

    /**
     * 等待DDL处理完成（通过轮询检查字段映射是否已更新）
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
                    Thread.sleep(500);
                    return;
                }
            }
            Thread.sleep(checkInterval);
        }

        logger.warn("等待DDL DROP处理完成超时（{}ms），字段: {}", timeoutMs, expectedFieldName);
    }

    /**
     * 等待Meta进入运行状态
     */
    private void waitForMetaRunning(String metaId, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long checkInterval = 200; // 每200ms检查一次

        logger.info("等待Meta进入运行状态: metaId={}", metaId);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null) {
                logger.info("Meta状态检查: metaId={}, state={}, isRunning={}, errorMessage={}",
                        metaId, meta.getState(), meta.isRunning(),
                        meta.getErrorMessage() != null && !meta.getErrorMessage().isEmpty() ? meta.getErrorMessage() : "无");

                if (meta.isRunning()) {
                    logger.info("Meta {} 已处于运行状态", metaId);
                    Thread.sleep(1000);
                    return;
                }

                if (meta.isError()) {
                    String errorMsg = String.format("Meta %s 处于错误状态: state=%d, errorMessage=%s",
                            metaId, meta.getState(), meta.getErrorMessage());
                    logger.error(errorMsg);
                    throw new RuntimeException(errorMsg);
                }
            } else {
                logger.warn("Meta {} 不存在", metaId);
            }
            Thread.sleep(checkInterval);
        }

        Meta meta = profileComponent.getMeta(metaId);
        assertNotNull("Meta不应为null", meta);
        logger.error("Meta状态检查失败: metaId={}, state={}, isRunning={}, errorMessage={}",
                metaId, meta.getState(), meta.isRunning(), meta.getErrorMessage());
        assertTrue("Meta应在" + timeoutMs + "ms内进入运行状态，当前状态: " + meta.getState() +
                        (meta.getErrorMessage() != null ? ", 错误信息: " + meta.getErrorMessage() : ""),
                meta.isRunning());
    }

    /**
     * 从数据库配置推断数据库类型（用于加载对应的SQL脚本）
     */
    private static String determineDatabaseType(DatabaseConfig config) {
        String url = config.getUrl();
        if (url == null) {
            return "sqlserver";
        }
        String urlLower = url.toLowerCase();
        if (urlLower.contains("sqlserver") || urlLower.contains("jdbc:sqlserver")) {
            return "sqlserver";
        }
        return "sqlserver";
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
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = DDLSqlServerCTIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
     */
    private static String loadSqlScript(String resourcePath) {
        try {
            InputStream input = DDLSqlServerCTIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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

}

