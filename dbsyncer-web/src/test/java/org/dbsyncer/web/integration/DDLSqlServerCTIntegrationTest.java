package org.dbsyncer.web.integration;

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

import static org.junit.Assert.*;

/**
 * SQL Server Change Tracking (CT) 到 SQL Server CT 的 DDL 同步集成测试
 * 全面测试 SQL Server CT 模式之间 DDL 同步的端到端功能，包括解析、转换和执行
 * 覆盖场景：
 * - ADD COLUMN: 基础添加、带约束、带NULL/NOT NULL
 * - DROP COLUMN: 删除字段
 * - ALTER COLUMN: 修改类型、修改长度、修改约束（NULL/NOT NULL）
 * - RENAME COLUMN: 重命名字段（使用 sp_rename，CT 模式特有功能）
 * - 异常处理
 * <p>
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

        String sqlServerDDL = "ALTER TABLE ddlTestSource ADD salary DECIMAL(10,2)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500); // 等待版本号更新

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        // 这个 INSERT 包含 DDL 新增的 salary 字段，用于验证 DDL 和 DML 的数据绑定关系
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData.put("salary", 5000.00); // DDL 新增的字段
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("salary", 10000);

        // 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundSalaryMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "salary".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "salary".equals(fm.getTarget().getName()));

        assertTrue("应找到salary字段的映射", foundSalaryMapping);
        verifyFieldExistsInTargetDatabase("salary", getTargetTableName(), targetConfig);

        // 验证 DML 数据同步：验证插入的数据（包含新字段）是否同步到目标表
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("ADD COLUMN基础测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试ADD COLUMN - 带NOT NULL约束
     */
    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestSource ADD phone NVARCHAR(20) NOT NULL DEFAULT ''";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500);

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData.put("phone", "13800138000"); // DDL 新增的字段（NOT NULL）
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        waitForDDLProcessingComplete("phone", 10000);

        // 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPhoneMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));

        assertTrue("应找到phone字段的映射", foundPhoneMapping);
        verifyFieldExistsInTargetDatabase("phone", getTargetTableName(), targetConfig);

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("ADD COLUMN带NOT NULL约束测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试通过fallbackQuery检测DDL（processedCount == 0场景）
     * 
     * 场景说明：
     * - 执行DDL操作后，在测试表上不执行任何DML操作
     * - 但在其他表上执行DML操作来增加Change Tracking版本号，触发Worker轮询
     * - 当Worker检测到版本号变化时，会调用pull方法查询所有表
     * - 对于测试表，Change Tracking查询返回0条DML记录（processedCount == 0）
     * - 系统通过fallbackQuery（buildSchemeOnly）检测DDL变更
     * - 验证DDL能够被正确检测并同步
     * 
     * 这是对SqlServerCTListener.processDMLResultSet方法中420-435行逻辑的测试。
     * 注意：此测试的重点是验证fallbackQuery机制本身，而不是验证具体的DDL类型。
     * 各种DDL类型（ADD/DROP/ALTER COLUMN等）的正常流程（有DML触发的情况）已在其他测试中覆盖。
     * 这里使用ADD COLUMN作为示例，因为最容易验证（检查字段是否存在）。
     * 
     * 关键点：
     * - Change Tracking的版本号只有在有DML操作时才会增加
     * - Worker只有在maxVersion > lastVersion时才会调用pull方法
     * - 如果版本号不增加，系统不会触发pull，也就不会执行fallbackQuery
     * - 因此需要在其他表上执行DML来增加版本号，触发pull方法
     */
    @Test
    public void testAddColumn_DetectedByFallbackQuery_NoDMLAfterDDL() throws Exception {
        logger.info("开始测试通过fallbackQuery检测DDL（processedCount == 0场景）");

        String sqlServerDDL = "ALTER TABLE ddlTestSource ADD remark NVARCHAR(200)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要先初始化表结构快照
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500); // 等待版本号更新

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 关键：在其他表执行 DML 操作来增加 Change Tracking 版本号
        // 注意：必须在非测试表上执行 DML，因为：
        // - Change Tracking 的版本号只有在有 DML 操作时才会增加
        // - Worker 只有在 maxVersion > lastVersion 时才会调用 pull 方法
        // - 如果版本号不增加，系统不会触发 pull，也就不会执行 fallbackQuery
        // - 在测试表上不执行 DML，这样测试表的 Change Tracking 查询会返回 0 条记录（processedCount == 0）
        // - 当 processedCount == 0 时，系统会通过 fallbackQuery 检测 DDL 变更
        
        // 创建一个临时表用于触发版本号变化（需要启用 Change Tracking）
        String triggerTableName = "ddlTestTriggerTable";
        String createTriggerTableDDL = String.format(
            "IF OBJECT_ID('%s', 'U') IS NULL BEGIN " +
            "CREATE TABLE %s (id INT IDENTITY(1,1) PRIMARY KEY, trigger_col NVARCHAR(50)); " +
            "ALTER TABLE %s ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON); " +
            "END",
            triggerTableName, triggerTableName, triggerTableName);
        executeDDLToSourceDatabase(createTriggerTableDDL, sourceConfig);
        
        // 在临时表上执行 INSERT 操作来触发 Change Tracking 版本号增加
        // 然后立即删除，确保不影响测试环境
        String insertTriggerDML = String.format("INSERT INTO %s (trigger_col) VALUES ('trigger')", triggerTableName);
        executeDDLToSourceDatabase(insertTriggerDML, sourceConfig);
        String deleteTriggerDML = String.format("DELETE FROM %s WHERE trigger_col = 'trigger'", triggerTableName);
        executeDDLToSourceDatabase(deleteTriggerDML, sourceConfig);
        
        // 这样 Worker 会检测到版本号变化，调用 pull 方法
        // 在 pull 方法中，对于测试表（ddlTestSource），由于没有实际的 DML 变更，processedCount == 0
        // 系统会执行 fallbackQuery 检测 DDL 变更

        // 4. 等待系统下一次轮询并检测到 DDL 变更
        // 注意：需要等待足够的时间让系统完成 Change Tracking 轮询
        // 轮询间隔通常是几秒，所以等待时间应该足够长
        Thread.sleep(3000); // 等待系统完成 Change Tracking 轮询

        // 5. 等待DDL处理完成（通过fallbackQuery检测）
        waitForDDLProcessingComplete("remark", 15000);

        // 6. 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundRemarkMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "remark".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "remark".equals(fm.getTarget().getName()));

        assertTrue("应找到remark字段的映射（通过fallbackQuery检测）", foundRemarkMapping);
        verifyFieldExistsInTargetDatabase("remark", getTargetTableName(), targetConfig);

        // 7. 验证可以正常插入包含新字段的数据（确保DDL同步成功）
        Map<String, Object> testData = new HashMap<>();
        testData.put("first_name", "Test");
        testData.put("last_name", "User");
        testData.put("department", "IT");
        testData.put("remark", "Test remark field"); // DDL 新增的字段
        testData = executeInsertDMLToSourceDatabase(getSourceTableName(), testData, sourceConfig);
        waitForDataSync(testData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("通过fallbackQuery检测DDL测试通过（processedCount == 0场景验证完成）");
    }

    // ==================== DROP COLUMN 测试场景 ====================

    /**
     * 测试DROP COLUMN - 删除字段
     */
    @Test
    public void testDropColumn_Basic() throws Exception {
        logger.info("开始测试DROP COLUMN - 删除字段");

        String sqlServerDDL = "ALTER TABLE ddlTestSource DROP COLUMN department";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT"); // 这个字段将被删除
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500);

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测（不包含被删除的字段）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "User");
        // 注意：不包含 department 字段，因为它已被删除
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        // 等待DDL DROP处理完成（使用轮询方式）
        waitForDDLDropProcessingComplete("department", 10000);

        // 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundDepartmentMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "department".equals(fm.getSource().getName()));

        assertFalse("不应找到department字段的映射", foundDepartmentMapping);
        verifyFieldNotExistsInTargetDatabase("department", getTargetTableName(), targetConfig);

        // 验证 DML 数据同步（不包含被删除的字段）
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("DROP COLUMN测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    // ==================== ALTER COLUMN 测试场景 ====================

    /**
     * 测试ALTER COLUMN - 修改字段长度
     */
    @Test
    public void testAlterColumn_ChangeLength() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段长度");

        String sqlServerDDL = "ALTER TABLE ddlTestSource ALTER COLUMN first_name NVARCHAR(100)";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500);

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测（使用修改后的字段长度）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "VeryLongFirstNameThatExceedsOriginalLength"); // 使用更长的值测试长度修改
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        Thread.sleep(2000);

        // 验证 DDL：字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("ALTER COLUMN修改长度测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试ALTER COLUMN - 修改字段类型
     */
    @Test
    public void testAlterColumn_ChangeType() throws Exception {
        logger.info("开始测试ALTER COLUMN - 修改字段类型");

        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestSource ADD count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        // 2. 执行第一个 DDL 操作（添加字段）
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测
        Map<String, Object> dataAfterAdd = new HashMap<>();
        dataAfterAdd.put("first_name", "Test");
        dataAfterAdd.put("last_name", "User");
        dataAfterAdd.put("department", "IT");
        dataAfterAdd.put("count_num", 100); // 新添加的字段（INT类型）
        dataAfterAdd = executeInsertDMLToSourceDatabase(getSourceTableName(), dataAfterAdd, sourceConfig);
        waitForDDLProcessingComplete("count_num", 10000);

        // 4. 执行第二个 DDL 操作（修改字段类型）
        String sqlServerDDL = "ALTER TABLE ddlTestSource ALTER COLUMN count_num BIGINT";
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 5. 执行 DML 操作来触发 DDL 检测（使用 BIGINT 类型的值）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData.put("count_num", 9999999999L); // 使用 BIGINT 类型的值
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        Thread.sleep(2000);

        // 验证 DDL：字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCountNumMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "count_num".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "count_num".equals(fm.getTarget().getName()));

        assertTrue("应找到count_num字段的映射", foundCountNumMapping);

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("ALTER COLUMN修改类型测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试ALTER COLUMN - 添加NOT NULL约束
     */
    @Test
    public void testAlterColumn_AddNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 添加NOT NULL约束");

        String sqlServerDDL = "ALTER TABLE ddlTestSource ALTER COLUMN last_name NVARCHAR(50) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500);

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测（使用 NOT NULL 的值）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "NotNullUser"); // NOT NULL 约束的值
        insertedData.put("department", "IT");
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        Thread.sleep(2000);

        // 验证 DDL：字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundLastNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "last_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "last_name".equals(fm.getTarget().getName()));

        assertTrue("应找到last_name字段的映射", foundLastNameMapping);

        // 验证目标表结构：字段应为 NOT NULL
        verifyFieldNotNull("last_name", getTargetTableName(), targetConfig);

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("ALTER COLUMN添加NOT NULL约束测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试ALTER COLUMN - 移除NOT NULL约束（允许NULL）
     */
    @Test
    public void testAlterColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试ALTER COLUMN - 移除NOT NULL约束");

        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestSource ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        executeDDLToSourceDatabase(setNotNullDDL, sourceConfig);

        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        Map<String, Object> dataAfterNotNull = new HashMap<>();
        dataAfterNotNull.put("first_name", "NotNull");
        dataAfterNotNull.put("last_name", "User");
        dataAfterNotNull.put("department", "IT");
        executeInsertDMLToSourceDatabase(getSourceTableName(), dataAfterNotNull, sourceConfig);
        Thread.sleep(2000);

        // 验证目标表结构：设置 NOT NULL 后，字段应为 NOT NULL
        verifyFieldNotNull("first_name", getTargetTableName(), targetConfig);

        // 4. 执行第二个 DDL 操作（移除 NOT NULL）
        String sqlServerDDL = "ALTER TABLE ddlTestSource ALTER COLUMN first_name NVARCHAR(50) NULL";
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 5. 执行 DML 操作来触发 DDL 检测（使用 NULL 值测试）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test"); // 仍然使用非NULL值（因为 INSERT 需要值）
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);
        Thread.sleep(2000);

        // 验证 DDL：字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        // 验证目标表结构：字段应为 NULL（可空）
        verifyFieldNullable("first_name", getTargetTableName(), targetConfig);

        logger.info("ALTER COLUMN移除NOT NULL约束测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    // ==================== RENAME COLUMN 测试场景 ====================

    /**
     * 测试RENAME COLUMN - 重命名字段（仅重命名，不修改类型）
     * CT 模式特有功能：通过列属性匹配检测重命名
     */
    @Test
    public void testRenameColumn_RenameOnly() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名字段");

        String sqlServerDDL = "EXEC sp_rename 'ddlTestSource.first_name', 'full_name', 'COLUMN'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init"); // 使用旧字段名
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500);

        // 2. 执行 DDL 操作（重命名字段）
        executeDDLToSourceDatabase(sqlServerDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测（使用新字段名）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("full_name", "Test"); // 使用新字段名
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        // 等待RENAME COLUMN处理完成（使用轮询方式）
        waitForDDLProcessingComplete("full_name", 10000);

        // 验证 DDL：字段映射
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

        // 验证 DML 数据同步（使用新字段名）
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("RENAME COLUMN重命名字段测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    /**
     * 测试RENAME COLUMN - 重命名并修改类型（先重命名，再修改类型）
     * 这种情况会生成 RENAME + ALTER 两个操作
     */
    @Test
    public void testRenameColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名并修改类型");

        // 先添加一个VARCHAR字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestSource ADD description NVARCHAR(100)";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        waitForMetaRunning(metaId, 10000);

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData.put("last_name", "User");
        initData.put("department", "IT");
        initData = executeInsertDMLToSourceDatabase(getSourceTableName(), initData, sourceConfig);
        waitForDataSync(initData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证初始化数据同步

        // 2. 执行第一个 DDL 操作（添加字段）
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);

        // 3. 执行 DML 操作来触发 DDL 检测
        Map<String, Object> dataAfterAdd = new HashMap<>();
        dataAfterAdd.put("first_name", "Test");
        dataAfterAdd.put("last_name", "User");
        dataAfterAdd.put("department", "IT");
        dataAfterAdd.put("description", "Original description"); // 新添加的字段
        dataAfterAdd = executeInsertDMLToSourceDatabase(getSourceTableName(), dataAfterAdd, sourceConfig);
        waitForDDLProcessingComplete("description", 10000);

        // 4. 执行第二个 DDL 操作（重命名）
        String renameDDL = "EXEC sp_rename 'ddlTestSource.description', 'desc_text', 'COLUMN'";
        executeDDLToSourceDatabase(renameDDL, sourceConfig);

        // 5. 执行 DML 操作来触发 DDL 检测（使用新字段名）
        Map<String, Object> dataAfterRename = new HashMap<>();
        dataAfterRename.put("first_name", "Test");
        dataAfterRename.put("last_name", "User");
        dataAfterRename.put("department", "IT");
        dataAfterRename.put("desc_text", "Renamed description"); // 使用新字段名
        dataAfterRename = executeInsertDMLToSourceDatabase(getSourceTableName(), dataAfterRename, sourceConfig);
        waitForDDLProcessingComplete("desc_text", 10000);

        // 6. 执行第三个 DDL 操作（修改类型）
        String alterDDL = "ALTER TABLE ddlTestSource ALTER COLUMN desc_text TEXT";
        executeDDLToSourceDatabase(alterDDL, sourceConfig);

        // 7. 执行 DML 操作来触发 DDL 检测（使用 TEXT 类型的值）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("last_name", "User");
        insertedData.put("department", "IT");
        insertedData.put("desc_text", "Long text content for TEXT type"); // TEXT 类型的值
        insertedData = executeInsertDMLToSourceDatabase(getSourceTableName(), insertedData, sourceConfig);

        Thread.sleep(2000);

        // 验证 DDL：字段映射
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

        // 验证 DML 数据同步
        waitForDataSync(insertedData, getTargetTableName(), "id", targetConfig, 10000); // 等待并验证数据同步

        logger.info("RENAME COLUMN重命名并修改类型测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    // ==================== CREATE TABLE 测试场景 ====================

    /**
     * 测试CREATE TABLE - IDENTITY列重复约束问题
     * 验证SQL Server到SQL Server同步时，包含IDENTITY列的表创建不会出现重复的IDENTITY约束
     * 
     * 问题场景：
     * - 源表包含IDENTITY列（如：INT IDENTITY(1,1)）
     * - 生成的CREATE TABLE DDL中可能出现：INT IDENTITY NOT NULL IDENTITY(1,1)
     * - 导致SQL错误：为表 'TestTableTwo' 的列 'ID' 指定了多个列 IDENTITY 约束
     * 
     * 修复验证：
     * - 生成的DDL应该是：INT NOT NULL IDENTITY(1,1)（没有重复的IDENTITY关键字）
     * - 表应该能够成功创建（这是核心验证点，如果DDL有重复IDENTITY，创建会失败）
     * - IDENTITY列属性应该正确设置
     */
    @Test
    public void testCreateTable_WithIdentityColumn_NoDuplicateIdentity() throws Exception {
        logger.info("开始测试CREATE TABLE - IDENTITY列重复约束问题");

        String testSourceTable = "TestTableOne";
        String testTargetTable = "TestTableTwo";

        // 准备：确保表不存在（复用MySQLToSQLServerDDLSyncIntegrationTest中的方法）
        prepareForCreateTableTest(testSourceTable, testTargetTable);

        // 1. 在源库创建包含IDENTITY列的表（模拟用户报告的问题场景）
        String createSourceTableDDL = String.format(
            "CREATE TABLE %s (\n" +
            "    [ID] INT IDENTITY(1,1) NOT NULL,\n" +
            "    [UserName] NVARCHAR(50) NOT NULL,\n" +
            "    [Age] INT NOT NULL,\n" +
            "    [RegisterTime] DATETIME NOT NULL,\n" +
            "    [Score] DECIMAL(5,2) NOT NULL,\n" +
            "    [IsActive] BIT NOT NULL,\n" +
            "    PRIMARY KEY ([UserName])\n" +
            ")", testSourceTable);
        
        executeDDLToSourceDatabase(createSourceTableDDL, sourceConfig);

        // 2. 使用createTargetTableFromSource方法创建目标表
        // 核心验证：如果DDL包含重复的IDENTITY约束，这里会抛出异常
        // createTargetTableFromSource内部已经验证了DDL不包含重复的IDENTITY关键字
        createTargetTableFromSource(testSourceTable, testTargetTable);

        // 3. 核心验证：表能够成功创建即说明没有重复IDENTITY约束
        // 简化验证：只验证关键点，其他详细验证已在createTargetTableFromSource中完成
        verifyTableExists(testTargetTable, targetConfig);
        verifyFieldExistsInTargetDatabase("ID", testTargetTable, targetConfig);
        verifyFieldNotNull("ID", testTargetTable, targetConfig);
        verifyTablePrimaryKeys(testTargetTable, targetConfig, Arrays.asList("UserName"));

        // 4. 验证可以正常插入数据（确保表结构正确，没有语法错误）
        Map<String, Object> testData = new HashMap<>();
        testData.put("UserName", "TestUser");
        testData.put("Age", 25);
        testData.put("RegisterTime", new java.util.Date());
        testData.put("Score", 95.50);
        testData.put("IsActive", true);
        testData = executeInsertDMLToSourceDatabase(testSourceTable, testData, sourceConfig);
        assertNotNull("插入的数据应该包含生成的ID", testData.get("ID"));
        
        logger.info("CREATE TABLE IDENTITY列重复约束问题测试通过（表创建成功，无重复IDENTITY约束）");
    }

    // ==================== 辅助方法 ====================

    /**
     * 从源表创建目标表（用于CREATE TABLE测试）
     * 验证generateCreateTableDDL方法生成的DDL是否正确，特别是IDENTITY列不重复的问题
     * 
     * 注意：此方法针对SQL Server -> SQL Server同构场景，简化了MySQLToSQLServerDDLSyncIntegrationTest中的逻辑
     */
    private void createTargetTableFromSource(String sourceTable, String targetTable) throws Exception {
        logger.info("开始从源表创建目标表: {} -> {}", sourceTable, targetTable);

        // 确保 connectorType 已设置
        if (sourceConfig.getConnectorType() == null) {
            sourceConfig.setConnectorType(getConnectorType(sourceConfig, true));
        }
        if (targetConfig.getConnectorType() == null) {
            targetConfig.setConnectorType(getConnectorType(targetConfig, false));
        }

        // 1. 连接源和目标数据库
        org.dbsyncer.sdk.connector.ConnectorInstance sourceConnectorInstance = connectorFactory.connect(sourceConfig);
        org.dbsyncer.sdk.connector.ConnectorInstance targetConnectorInstance = connectorFactory.connect(targetConfig);

        // 2. 检查目标表是否已存在
        try {
            org.dbsyncer.sdk.model.MetaInfo existingTable = connectorFactory.getMetaInfo(targetConnectorInstance, targetTable);
            if (existingTable != null && existingTable.getColumn() != null && !existingTable.getColumn().isEmpty()) {
                logger.info("目标表已存在，跳过创建: {}", targetTable);
                return;
            }
        } catch (Exception e) {
            logger.debug("目标表不存在，开始创建: {}", targetTable);
        }

        // 3. 获取源表结构
        org.dbsyncer.sdk.model.MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(sourceConnectorInstance, sourceTable);
        assertNotNull("无法获取源表结构: " + sourceTable, sourceMetaInfo);
        assertFalse("源表没有字段: " + sourceTable, sourceMetaInfo.getColumn() == null || sourceMetaInfo.getColumn().isEmpty());

        // 4. SQL Server -> SQL Server同构场景：直接使用源表结构，无需类型转换
        org.dbsyncer.sdk.spi.ConnectorService<?, ?> targetConnectorService = connectorFactory.getConnectorService(targetConfig.getConnectorType());
        
        // 生成 CREATE TABLE DDL（直接使用源表MetaInfo，因为同构数据库无需类型转换）
        String createTableDDL = targetConnectorService.generateCreateTableDDL(sourceMetaInfo, targetTable);

        // 5. 核心验证：检查生成的DDL中不包含重复的IDENTITY关键字
        assertNotNull("无法生成 CREATE TABLE DDL", createTableDDL);
        assertFalse("生成的 CREATE TABLE DDL 为空", createTableDDL.trim().isEmpty());
        
        String upperDDL = createTableDDL.toUpperCase();
        // 验证：不应该有 "IDENTITY" 后面紧跟着 "NOT NULL" 再跟着 "IDENTITY" 的模式
        // 这是修复的核心验证点
        assertFalse("生成的DDL中不应该包含重复的IDENTITY约束（如：IDENTITY NOT NULL IDENTITY(1,1)）",
                upperDDL.contains("IDENTITY") && upperDDL.contains("NOT NULL") && 
                upperDDL.indexOf("IDENTITY") < upperDDL.indexOf("NOT NULL") &&
                upperDDL.indexOf("NOT NULL") < upperDDL.lastIndexOf("IDENTITY"));

        // 6. 执行 CREATE TABLE DDL（如果DDL有重复IDENTITY，这里会失败）
        org.dbsyncer.sdk.config.DDLConfig ddlConfig = new org.dbsyncer.sdk.config.DDLConfig();
        ddlConfig.setSql(createTableDDL);
        org.dbsyncer.common.model.Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig, null);

        if (result != null && result.error != null && !result.error.trim().isEmpty()) {
            throw new RuntimeException("创建表失败: " + result.error + "\n生成的DDL: " + createTableDDL);
        }

        logger.info("成功创建目标表: {}，生成的DDL: {}", targetTable, createTableDDL);
    }

    /**
     * 准备建表测试环境（确保表不存在）
     * 使用基类的通用方法
     */
    private void prepareForCreateTableTest(String sourceTable, String targetTable) throws Exception {
        logger.debug("准备建表测试环境，确保表不存在: sourceTable={}, targetTable={}", sourceTable, targetTable);
        
        // SQL Server删除表的语法
        String dropSourceSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", sourceTable, sourceTable);
        String dropTargetSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", targetTable, targetTable);
        
        try {
            executeDDLToSourceDatabase(dropSourceSql, sourceConfig);
            executeDDLToSourceDatabase(dropTargetSql, targetConfig);
        } catch (Exception e) {
            logger.debug("清理测试表时出错（可能表不存在）: {}", e.getMessage());
        }
        
        Thread.sleep(200);
        logger.debug("建表测试环境准备完成");
    }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return DDLSqlServerCTIntegrationTest.class;
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
        return "ddlTestSource";
    }

    @Override
    protected String getTargetTableName() {
        return "ddlTestTarget";
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

