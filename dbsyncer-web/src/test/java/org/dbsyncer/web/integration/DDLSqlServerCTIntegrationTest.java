package org.dbsyncer.web.integration;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.web.Application;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Properties;

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
public class DDLSqlServerCTIntegrationTest extends BaseDDLIntegrationTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("开始初始化SQL Server CT到SQL Server CT的DDL同步测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境（使用按数据库类型分类的脚本）
        String initSql = loadSqlScriptByDatabaseTypeStatic("reset-test-table", "sqlserver", DDLSqlServerCTIntegrationTest.class);
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        // 注意：不需要手动启用 Change Tracking
        // SqlServerCTListener.start() 会自动调用 enableDBChangeTracking() 和 enableTableChangeTracking()

        logger.info("SQL Server CT到SQL Server CT的DDL同步测试环境初始化完成");
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
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

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server CT到SQL Server CT的DDL同步测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            String cleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "sqlserver", DDLSqlServerCTIntegrationTest.class);
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
        sourceConnectorId = createConnector(getSourceConnectorName(), sourceConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), targetConfig, false);

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

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照
        executeDMLToSourceDatabase("ddlTestEmployee", sourceConfig);
        Thread.sleep(500); // 等待版本号更新

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 再执行一次 DML 操作来触发 DDL 检测（版本号变化会触发 pull，从而检测 DDL 变更）
        executeDMLToSourceDatabase("ddlTestEmployee", sourceConfig);

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

    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return DDLSqlServerCTIntegrationTest.class;
    }

    @Override
    protected void loadTestConfig() throws IOException {
        loadTestConfigStatic();
    }

    @Override
    protected String getSourceConnectorName() {
        return "SQL Server CT源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "SQL Server CT目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "SQL Server CT到SQL Server CT测试Mapping";
    }

    @Override
    protected String getSourceTableName() {
        return "ddlTestEmployee";
    }

    @Override
    protected String getTargetTableName() {
        return "ddlTestEmployee";
    }

    @Override
    protected List<String> getInitialFieldMappings() {
        List<String> fieldMappingList = new ArrayList<>();
        fieldMappingList.add("id|id");
        fieldMappingList.add("first_name|first_name");
        fieldMappingList.add("last_name|last_name");
        fieldMappingList.add("department|department");
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        return "SqlServerCT"; // 使用 CT 模式
    }

    @Override
    protected String getIncrementStrategy() {
        return "Log"; // CT 模式使用日志监听
    }

    @Override
    protected String getDatabaseType(boolean isSource) {
        return "sqlserver"; // SQL Server 数据库类型
    }

}

