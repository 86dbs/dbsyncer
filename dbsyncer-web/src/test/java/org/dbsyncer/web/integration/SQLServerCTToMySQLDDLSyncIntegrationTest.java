package org.dbsyncer.web.integration;

import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
public class SQLServerCTToMySQLDDLSyncIntegrationTest extends BaseDDLIntegrationTest {

    private static DatabaseConfig sqlServerConfig;
    private static DatabaseConfig mysqlConfig;

    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("开始初始化SQL Server CT到MySQL的DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 设置基类的sourceConfig和targetConfig（用于基类方法）
        sourceConfig = sqlServerConfig;
        targetConfig = mysqlConfig;

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

        // 注意：不需要手动启用 Change Tracking
        // SqlServerCTListener.start() 会自动调用 enableDBChangeTracking() 和 enableTableChangeTracking()

        logger.info("SQL Server CT到MySQL的DDL同步集成测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理SQL Server CT到MySQL的DDL同步集成测试环境");

        try {
            String sourceCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "sqlserver", SQLServerCTToMySQLDDLSyncIntegrationTest.class);
            String targetCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "mysql", SQLServerCTToMySQLDDLSyncIntegrationTest.class);
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
        sourceConnectorId = createConnector(getSourceConnectorName(), sqlServerConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), mysqlConfig, false);

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

        // 注意：不需要在 tearDown() 中重置表结构
        // 因为下一个测试的 setUp() 会重置表结构，避免重复操作
        // 如果测试失败，下一个测试的 setUp() 也会确保从干净状态开始

        mappingId = null;
        metaId = null;
        sourceConnectorId = null;
        targetConnectorId = null;
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

    @Test
    public void testAddColumn_XMLType() throws Exception {
        logger.info("开始测试XML类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD metadata XML";
        testDDLConversion(sqlserverDDL, "metadata");
        // 验证：XML → LONGTEXT
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("metadata", tableGroup.getTargetTable().getName(), mysqlConfig, "longtext");
    }

    @Test
    public void testAddColumn_UNIQUEIDENTIFIERType() throws Exception {
        logger.info("开始测试UNIQUEIDENTIFIER类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD guid_id UNIQUEIDENTIFIER";
        testDDLConversion(sqlserverDDL, "guid_id");
        // 验证：UNIQUEIDENTIFIER → CHAR(36)
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("guid_id", tableGroup.getTargetTable().getName(), mysqlConfig, "char");
        verifyFieldLength("guid_id", tableGroup.getTargetTable().getName(), mysqlConfig, 36);
    }

    @Test
    public void testAddColumn_MONEYType() throws Exception {
        logger.info("开始测试MONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary MONEY";
        testDDLConversion(sqlserverDDL, "salary");
        // 验证：MONEY → DECIMAL(19,4)
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("salary", tableGroup.getTargetTable().getName(), mysqlConfig, "decimal");
    }

    @Test
    public void testAddColumn_SMALLMONEYType() throws Exception {
        logger.info("开始测试SMALLMONEY类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD bonus SMALLMONEY";
        testDDLConversion(sqlserverDDL, "bonus");
        // 验证：SMALLMONEY → DECIMAL(10,4)
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("bonus", tableGroup.getTargetTable().getName(), mysqlConfig, "decimal");
    }

    @Test
    public void testAddColumn_DATETIME2Type() throws Exception {
        logger.info("开始测试DATETIME2类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_at DATETIME2";
        testDDLConversion(sqlserverDDL, "created_at");
        // 验证：DATETIME2 → DATETIME
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("created_at", tableGroup.getTargetTable().getName(), mysqlConfig, "datetime");
    }

    @Test
    public void testAddColumn_DATETIMEOFFSETType() throws Exception {
        logger.info("开始测试DATETIMEOFFSET类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD updated_at DATETIMEOFFSET";
        
        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);
        
        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData = executeInsertDMLToSourceDatabase("ddlTestEmployee", initData, sqlServerConfig);
        waitForDataSync(initData, "ddlTestEmployee", "id", mysqlConfig, 10000); // 等待并验证初始化数据同步
        
        Thread.sleep(500); // 等待版本号更新
        
        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        // 这个 INSERT 包含 DDL 新增的 updated_at 字段，用于验证 DDL 和 DML 的数据绑定关系
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("updated_at", java.time.OffsetDateTime.now()); // DDL 新增的字段（DATETIMEOFFSET类型）
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("updated_at", 10000);
        
        // 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);
        
        boolean foundUpdatedAtMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "updated_at".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "updated_at".equals(fm.getTarget().getName()));
        
        assertTrue("应找到updated_at字段的映射", foundUpdatedAtMapping);
        verifyFieldExistsInTargetDatabase("updated_at", "ddlTestEmployee", mysqlConfig);
        
        // 验证：DATETIMEOFFSET → DATETIME
        // 注意：MySQL 的 DATETIME 不支持时区信息，所以时区信息会丢失
        // 如果需要保留时区信息，应该转换为 VARCHAR，但这需要修改类型映射逻辑
        verifyFieldType("updated_at", tableGroup.getTargetTable().getName(), mysqlConfig, "datetime");
        
        // 验证 DML 数据同步：验证插入的数据（包含新字段）是否同步到目标表
        waitForDataSync(insertedData, "ddlTestEmployee", "id", mysqlConfig, 10000); // 等待并验证数据同步
        
        logger.info("DATETIMEOFFSET类型转换测试通过（DDL 和 DML 数据绑定验证完成）");
    }

    @Test
    public void testAddColumn_TIMESTAMPType() throws Exception {
        logger.info("开始测试TIMESTAMP类型转换（rowversion）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD rowversion TIMESTAMP";
        testDDLConversion(sqlserverDDL, "rowversion");
        // 验证：TIMESTAMP (rowversion) → BIGINT
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("rowversion", tableGroup.getTargetTable().getName(), mysqlConfig, "bigint");
    }

    @Test
    public void testAddColumn_IMAGEType() throws Exception {
        logger.info("开始测试IMAGE类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD photo IMAGE";
        testDDLConversion(sqlserverDDL, "photo");
        // 验证：IMAGE → LONGBLOB
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("photo", tableGroup.getTargetTable().getName(), mysqlConfig, "longblob");
    }

    @Test
    public void testAddColumn_TEXTType() throws Exception {
        logger.info("开始测试TEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD description TEXT";
        testDDLConversion(sqlserverDDL, "description");
        // 验证：TEXT → LONGTEXT（SQL Server TEXT 容量为 2GB，大于 MySQL MEDIUMTEXT 的 16MB，所以转换为 LONGTEXT）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("description", tableGroup.getTargetTable().getName(), mysqlConfig, "longtext");
    }

    @Test
    public void testAddColumn_NTEXTType() throws Exception {
        logger.info("开始测试NTEXT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD content NTEXT";
        testDDLConversion(sqlserverDDL, "content");
        // 验证：NTEXT → LONGTEXT（SQL Server NTEXT 容量为 1GB，大于 MySQL MEDIUMTEXT 的 16MB，所以转换为 LONGTEXT）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("content", tableGroup.getTargetTable().getName(), mysqlConfig, "longtext");
    }

    @Test
    public void testAddColumn_BITType() throws Exception {
        logger.info("开始测试BIT类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD is_active BIT";
        testDDLConversion(sqlserverDDL, "is_active");
        // 验证：BIT → TINYINT
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("is_active", tableGroup.getTargetTable().getName(), mysqlConfig, "tinyint");
    }

    @Test
    public void testAddColumn_SMALLDATETIMEType() throws Exception {
        logger.info("开始测试SMALLDATETIME类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD event_time SMALLDATETIME";
        testDDLConversion(sqlserverDDL, "event_time");
        // 验证：SMALLDATETIME → DATETIME
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("event_time", tableGroup.getTargetTable().getName(), mysqlConfig, "datetime");
    }

    @Test
    public void testAddColumn_VARCHARMAXType() throws Exception {
        logger.info("开始测试VARCHAR(MAX)类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD large_text VARCHAR(MAX)";
        testDDLConversion(sqlserverDDL, "large_text");
        // 验证：VARCHAR(MAX) → LONGTEXT
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("large_text", tableGroup.getTargetTable().getName(), mysqlConfig, "longtext");
    }

    @Test
    public void testAddColumn_NVARCHARMAXType() throws Exception {
        logger.info("开始测试NVARCHAR(MAX)类型转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD unicode_text NVARCHAR(MAX)";
        testDDLConversion(sqlserverDDL, "unicode_text");
        // 验证：NVARCHAR(MAX) → LONGTEXT
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("unicode_text", tableGroup.getTargetTable().getName(), mysqlConfig, "longtext");
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
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 执行第一个 DDL 操作（添加字段）
        executeDDLToSourceDatabase(addColumnDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测
        triggerDDLDetection("count_num");
        waitForDDLProcessingComplete("count_num", 10000);

        // 4. 执行第二个 DDL 操作（修改字段类型）
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN count_num BIGINT";
        testDDLConversion(sqlserverDDL, "count_num");
    }

    @Test
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN操作 - 移除NOT NULL约束");
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NOT NULL";
        executeDDLToSourceDatabase(setNotNullDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测
        triggerDDLDetection(null);
        Thread.sleep(2000);

        // 4. 执行第二个 DDL 操作（移除 NOT NULL）
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(50) NULL";
        testDDLConversion(sqlserverDDL, "first_name");

        // 验证字段可空（NULL约束）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldNullable("first_name", tableGroup.getTargetTable().getName(), mysqlConfig);
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

        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlServerDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测（使用新字段名）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("full_name", "Test"); // 使用新字段名
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);

        // 4. 等待RENAME COLUMN处理完成（使用轮询方式）
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

    /**
     * 测试RENAME COLUMN - 重命名并修改类型（先重命名，再修改类型）
     * 这种情况会生成 RENAME + ALTER 两个操作
     */
    @Test
    public void testRenameColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名并修改类型");

        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 先添加一个NVARCHAR字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestEmployee ADD description NVARCHAR(100)";
        executeDDLToSourceDatabase(addColumnDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测
        triggerDDLDetection("description");
        waitForDDLProcessingComplete("description", 10000);

        // 4. 执行 RENAME 操作
        String renameDDL = "EXEC sp_rename 'ddlTestEmployee.description', 'desc_text', 'COLUMN'";
        executeDDLToSourceDatabase(renameDDL, sqlServerConfig);
        
        // 5. 执行 DML 操作来触发 DDL 检测（使用新字段名）
        Map<String, Object> dataAfterRename = new HashMap<>();
        dataAfterRename.put("first_name", "Test");
        dataAfterRename.put("desc_text", "Renamed description"); // 使用新字段名
        dataAfterRename = executeInsertDMLToSourceDatabase("ddlTestEmployee", dataAfterRename, sqlServerConfig);
        waitForDDLProcessingComplete("desc_text", 10000);

        // 6. 执行 ALTER COLUMN 修改类型
        String alterDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN desc_text TEXT";
        executeDDLToSourceDatabase(alterDDL, sqlServerConfig);
        
        // 7. 执行 DML 操作来触发 DDL 检测（使用 TEXT 类型的值）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("desc_text", "Long text content for TEXT type"); // TEXT 类型的值
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);
        Thread.sleep(2000);

        // 验证字段映射
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

        // 验证字段类型已修改为TEXT
        verifyFieldType("desc_text", tableGroup.getTargetTable().getName(), mysqlConfig, "text");

        logger.info("RENAME COLUMN重命名并修改类型测试通过");
    }

    /**
     * 测试RENAME COLUMN - 重命名并修改长度和约束
     */
    @Test
    public void testRenameColumn_RenameAndModifyLengthAndConstraint() throws Exception {
        logger.info("开始测试RENAME COLUMN - 重命名并修改长度和约束");

        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 执行 RENAME 操作
        String renameDDL = "EXEC sp_rename 'ddlTestEmployee.first_name', 'user_name', 'COLUMN'";
        executeDDLToSourceDatabase(renameDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测（使用新字段名）
        Map<String, Object> dataAfterRename = new HashMap<>();
        dataAfterRename.put("user_name", "Test"); // 使用新字段名
        dataAfterRename = executeInsertDMLToSourceDatabase("ddlTestEmployee", dataAfterRename, sqlServerConfig);
        waitForDDLProcessingComplete("user_name", 10000);

        // 4. 执行 ALTER COLUMN 修改长度和约束
        String alterDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN user_name NVARCHAR(100) NOT NULL";
        executeDDLToSourceDatabase(alterDDL, sqlServerConfig);
        
        // 5. 执行 DML 操作来触发 DDL 检测
        triggerDDLDetection(null);
        Thread.sleep(2000);

        // 验证字段映射
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        // 验证新映射存在
        boolean foundNewMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "user_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "user_name".equals(fm.getTarget().getName()));

        // 验证旧映射不存在
        boolean notFoundOldMapping = tableGroup.getFieldMapping().stream()
                .noneMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到user_name到user_name的字段映射", foundNewMapping);
        assertTrue("不应找到first_name到first_name的旧字段映射", notFoundOldMapping);

        // 验证字段长度和NOT NULL约束
        verifyFieldLength("user_name", tableGroup.getTargetTable().getName(), mysqlConfig, 100);
        verifyFieldNotNull("user_name", tableGroup.getTargetTable().getName(), mysqlConfig);

        logger.info("RENAME COLUMN重命名并修改长度和约束测试通过");
    }

    // ==================== 复杂场景测试 ====================

    @Test
    public void testAddMultipleColumns() throws Exception {
        logger.info("开始测试多字段同时添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2), bonus DECIMAL(8,2)";

        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("salary", 5000.00); // DDL 新增的字段
        insertedData.put("bonus", 1000.00); // DDL 新增的字段
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);

        // 4. 等待DDL处理完成
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

    /**
     * 测试ADD COLUMN - 带DEFAULT值（源DDL中的DEFAULT值会被忽略）
     * 注意：根据2.7.0版本的设计，缺省值处理已被忽略，因为各数据库缺省值函数表达差异很大
     * 源DDL中的DEFAULT值在解析时会被跳过，不会同步到目标数据库
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试带默认值的字段添加（源DDL中的DEFAULT值会被忽略）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD status NVARCHAR(20) DEFAULT 'active'";
        testDDLConversion(sqlserverDDL, "status");
        
        // 验证：源DDL中的DEFAULT值不会被同步，目标字段不应该有DEFAULT值（除非是NOT NULL字段自动生成的）
        // status字段是可空的，所以不应该有DEFAULT值
        // 注意：这里不验证DEFAULT值，因为源DDL中的DEFAULT值已被忽略（AbstractSourceToIRConverter会跳过DEFAULT值）
    }

    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试带NOT NULL约束的字段添加");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD phone NVARCHAR(20) NOT NULL";
        testDDLConversion(sqlserverDDL, "phone");
        
        // 验证：NOT NULL字段会自动添加DEFAULT值（仅为了满足MySQL语法要求）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldDefaultValue("phone", tableGroup.getTargetTable().getName(), mysqlConfig, "''");
    }

    /**
     * 测试ADD COLUMN - 带DEFAULT值和NOT NULL约束（源DDL中的DEFAULT值会被忽略）
     * 注意：源DDL中的DEFAULT值在解析时会被跳过，目标数据库会使用自动生成的DEFAULT值（仅为了满足语法要求）
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试带默认值和NOT NULL约束的字段添加（源DDL中的DEFAULT值会被忽略）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_by NVARCHAR(50) NOT NULL DEFAULT 'system'";
        testDDLConversion(sqlserverDDL, "created_by");
        
        // 验证：源DDL中的DEFAULT 'system'会被忽略，目标数据库会使用自动生成的DEFAULT ''（仅为了满足语法要求）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldDefaultValue("created_by", tableGroup.getTargetTable().getName(), mysqlConfig, "''");
        verifyFieldNotNull("created_by", tableGroup.getTargetTable().getName(), mysqlConfig);
    }

    // ==================== 数据类型边界测试 ====================

    /**
     * 测试DECIMAL精度转换
     * 验证SQL Server DECIMAL(19,4) → MySQL DECIMAL(19,4)的精度保持
     */
    @Test
    public void testAddColumn_DECIMALPrecision() throws Exception {
        logger.info("开始测试DECIMAL精度转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD amount DECIMAL(19,4)";
        testDDLConversion(sqlserverDDL, "amount");
        
        // 验证精度
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("amount", tableGroup.getTargetTable().getName(), mysqlConfig, "decimal");
        // 注意：MySQL的INFORMATION_SCHEMA中NUMERIC_PRECISION和NUMERIC_SCALE可以验证精度
    }

    /**
     * 测试DATETIME2精度转换
     * 验证SQL Server DATETIME2(7) → MySQL DATETIME（精度会被截断）
     */
    @Test
    public void testAddColumn_DATETIME2Precision() throws Exception {
        logger.info("开始测试DATETIME2精度转换");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD precise_time DATETIME2(7)";
        testDDLConversion(sqlserverDDL, "precise_time");
        
        // 验证：DATETIME2(7) → DATETIME（MySQL的DATETIME精度是秒级）
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        verifyFieldType("precise_time", tableGroup.getTargetTable().getName(), mysqlConfig, "datetime");
    }

    /**
     * 测试VARCHAR长度边界
     * 验证不同长度的VARCHAR转换
     */
    @Test
    public void testAddColumn_VARCHARLengthBoundary() throws Exception {
        logger.info("开始测试VARCHAR长度边界");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD short_text VARCHAR(255), long_text VARCHAR(4000)";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("short_text", "Short");
        insertedData.put("long_text", "Long text content");
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);
        
        // 4. 等待DDL处理完成
        waitForDDLProcessingComplete(Arrays.asList("short_text", "long_text"), 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("short_text", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldExistsInTargetDatabase("long_text", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldLength("short_text", tableGroup.getTargetTable().getName(), mysqlConfig, 255);
        verifyFieldLength("long_text", tableGroup.getTargetTable().getName(), mysqlConfig, 4000);
        
        logger.info("VARCHAR长度边界测试通过");
    }

    /**
     * 测试NVARCHAR长度边界
     * 验证不同长度的NVARCHAR转换
     */
    @Test
    public void testAddColumn_NVARCHARLengthBoundary() throws Exception {
        logger.info("开始测试NVARCHAR长度边界");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD unicode_short NVARCHAR(255), unicode_long NVARCHAR(2000)";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 3. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        insertedData.put("unicode_short", "短文本");
        insertedData.put("unicode_long", "长文本内容");
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);
        
        // 4. 等待DDL处理完成
        waitForDDLProcessingComplete(Arrays.asList("unicode_short", "unicode_long"), 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("unicode_short", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldExistsInTargetDatabase("unicode_long", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldLength("unicode_short", tableGroup.getTargetTable().getName(), mysqlConfig, 255);
        verifyFieldLength("unicode_long", tableGroup.getTargetTable().getName(), mysqlConfig, 2000);
        
        logger.info("NVARCHAR长度边界测试通过");
    }

    // ==================== 约束完整测试 ====================

    /**
     * 测试NOT NULL + 自动DEFAULT值（INT类型）
     * SQL Server添加NOT NULL字段时，MySQL应该自动添加DEFAULT 0
     */
    @Test
    public void testAddColumn_WithNotNull_INT() throws Exception {
        logger.info("开始测试INT类型NOT NULL字段（应自动添加DEFAULT 0）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD age INT NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("age");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("age", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("age", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldDefaultValue("age", tableGroup.getTargetTable().getName(), mysqlConfig, "0");
        verifyFieldNotNull("age", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("INT类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    /**
     * 测试NOT NULL + 自动DEFAULT值（BIGINT类型）
     */
    @Test
    public void testAddColumn_WithNotNull_BIGINT() throws Exception {
        logger.info("开始测试BIGINT类型NOT NULL字段（应自动添加DEFAULT 0）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD count_num BIGINT NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("count_num");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("count_num", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("count_num", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldDefaultValue("count_num", tableGroup.getTargetTable().getName(), mysqlConfig, "0");
        verifyFieldNotNull("count_num", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("BIGINT类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    /**
     * 测试NOT NULL + 自动DEFAULT值（DECIMAL类型）
     */
    @Test
    public void testAddColumn_WithNotNull_DECIMAL() throws Exception {
        logger.info("开始测试DECIMAL类型NOT NULL字段（应自动添加DEFAULT 0）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD price DECIMAL(10,2) NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("price");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("price", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("price", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldDefaultValue("price", tableGroup.getTargetTable().getName(), mysqlConfig, "0");
        verifyFieldNotNull("price", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("DECIMAL类型NOT NULL字段测试通过（已验证DEFAULT 0自动添加）");
    }

    /**
     * 测试NOT NULL + 自动DEFAULT值（DATE类型）
     */
    @Test
    public void testAddColumn_WithNotNull_DATE() throws Exception {
        logger.info("开始测试DATE类型NOT NULL字段（应自动添加DEFAULT '1900-01-01'）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD birth_date DATE NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("birth_date");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("birth_date", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("birth_date", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldDefaultValue("birth_date", tableGroup.getTargetTable().getName(), mysqlConfig, "'1900-01-01'");
        verifyFieldNotNull("birth_date", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("DATE类型NOT NULL字段测试通过（已验证DEFAULT '1900-01-01'自动添加）");
    }

    /**
     * 测试NOT NULL + 自动DEFAULT值（DATETIME2类型）
     */
    @Test
    public void testAddColumn_WithNotNull_DATETIME2() throws Exception {
        logger.info("开始测试DATETIME2类型NOT NULL字段（应自动添加DEFAULT '1900-01-01'）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD created_at DATETIME2 NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("created_at");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("created_at", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("created_at", tableGroup.getTargetTable().getName(), mysqlConfig);
        // DATETIME2 → DATETIME，默认值应该是 '1900-01-01 00:00:00' 或 '1900-01-01'
        verifyFieldDefaultValue("created_at", tableGroup.getTargetTable().getName(), mysqlConfig, "'1900-01-01'");
        verifyFieldNotNull("created_at", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("DATETIME2类型NOT NULL字段测试通过（已验证DEFAULT值自动添加）");
    }

    /**
     * 测试NOT NULL + 自动DEFAULT值（NVARCHAR类型）
     */
    @Test
    public void testAddColumn_WithNotNull_NVARCHAR() throws Exception {
        logger.info("开始测试NVARCHAR类型NOT NULL字段（应自动添加DEFAULT ''）");
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD code NVARCHAR(10) NOT NULL";
        
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();
        
        // 2. 清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列）
        clearTableData("ddlTestEmployee", sqlServerConfig);
        clearTableData("ddlTestEmployee", mysqlConfig);
        
        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sqlserverDDL, sqlServerConfig);
        
        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        triggerDDLDetection("code");
        
        // 5. 等待DDL处理完成
        waitForDDLProcessingComplete("code", 10000);
        
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);
        
        verifyFieldExistsInTargetDatabase("code", tableGroup.getTargetTable().getName(), mysqlConfig);
        verifyFieldDefaultValue("code", tableGroup.getTargetTable().getName(), mysqlConfig, "''");
        verifyFieldNotNull("code", tableGroup.getTargetTable().getName(), mysqlConfig);
        
        logger.info("NVARCHAR类型NOT NULL字段测试通过（已验证DEFAULT ''自动添加）");
    }

    // ==================== CREATE TABLE 测试场景 ====================

    /**
     * 测试CREATE TABLE - 基础建表（配置阶段）
     * 验证SQL Server到MySQL的类型转换和约束转换
     */
    @Test
    public void testCreateTable_Basic() throws Exception {
        logger.info("开始测试CREATE TABLE - 基础建表（配置阶段）");

        // 准备：确保表不存在
        prepareForCreateTableTest("createTableTestSource", "createTableTestTarget");

        // 先在源库创建表（SQL Server）
        String sourceDDL = "IF OBJECT_ID('createTableTestSource', 'U') IS NOT NULL DROP TABLE createTableTestSource;\n" +
                "CREATE TABLE createTableTestSource (\n" +
                "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                "    actID INT NOT NULL,\n" +
                "    pid INT NOT NULL,\n" +
                "    mediumID INT NOT NULL,\n" +
                "    createtime DATETIME2 NOT NULL\n" +
                ");";

        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("createTableTestSource", "createTableTestTarget");

        // 验证表结构
        verifyTableExists("createTableTestTarget", mysqlConfig);
        verifyTableFieldCount("createTableTestTarget", mysqlConfig, 5);
        verifyFieldExistsInTargetDatabase("id", "createTableTestTarget", mysqlConfig);
        verifyFieldExistsInTargetDatabase("actID", "createTableTestTarget", mysqlConfig);
        verifyFieldExistsInTargetDatabase("pid", "createTableTestTarget", mysqlConfig);
        verifyFieldExistsInTargetDatabase("mediumID", "createTableTestTarget", mysqlConfig);
        verifyFieldExistsInTargetDatabase("createtime", "createTableTestTarget", mysqlConfig);

        // 验证主键（IDENTITY → AUTO_INCREMENT）
        verifyTablePrimaryKeys("createTableTestTarget", mysqlConfig, Arrays.asList("id"));

        // 验证字段属性
        verifyFieldNotNull("id", "createTableTestTarget", mysqlConfig);
        verifyFieldNotNull("actID", "createTableTestTarget", mysqlConfig);
        verifyFieldType("createtime", "createTableTestTarget", mysqlConfig, "datetime");

        logger.info("CREATE TABLE基础建表测试通过");
    }

    /**
     * 测试CREATE TABLE - 带约束（NOT NULL、IDENTITY）
     */
    @Test
    public void testCreateTable_WithConstraints() throws Exception {
        logger.info("开始测试CREATE TABLE - 带约束");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableWithConstraints", "testTableWithConstraints");

        // 先在源库创建表（SQL Server，包含各种约束）
        String sourceDDL = "IF OBJECT_ID('testTableWithConstraints', 'U') IS NOT NULL DROP TABLE testTableWithConstraints;\n" +
                "CREATE TABLE testTableWithConstraints (\n" +
                "    id INT IDENTITY(1,1) PRIMARY KEY,\n" +
                "    username NVARCHAR(50) NOT NULL,\n" +
                "    email NVARCHAR(100),\n" +
                "    age INT NOT NULL,\n" +
                "    status TINYINT NOT NULL DEFAULT 1\n" +
                ");";

        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableWithConstraints", "testTableWithConstraints");

        // 验证表结构
        verifyTableExists("testTableWithConstraints", mysqlConfig);
        verifyTableFieldCount("testTableWithConstraints", mysqlConfig, 5);

        // 验证约束
        verifyFieldNotNull("id", "testTableWithConstraints", mysqlConfig);
        verifyFieldNotNull("username", "testTableWithConstraints", mysqlConfig);
        verifyFieldNotNull("age", "testTableWithConstraints", mysqlConfig);
        verifyFieldNotNull("status", "testTableWithConstraints", mysqlConfig);
        verifyFieldNullable("email", "testTableWithConstraints", mysqlConfig);

        // 验证主键（IDENTITY → AUTO_INCREMENT）
        verifyTablePrimaryKeys("testTableWithConstraints", mysqlConfig, Arrays.asList("id"));

        logger.info("CREATE TABLE带约束测试通过");
    }

    /**
     * 测试CREATE TABLE - 复合主键
     */
    @Test
    public void testCreateTable_WithCompositePrimaryKey() throws Exception {
        logger.info("开始测试CREATE TABLE - 复合主键");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableCompositePK", "testTableCompositePK");

        // 先在源库创建表（SQL Server，复合主键）
        String sourceDDL = "IF OBJECT_ID('testTableCompositePK', 'U') IS NOT NULL DROP TABLE testTableCompositePK;\n" +
                "CREATE TABLE testTableCompositePK (\n" +
                "    user_id INT NOT NULL,\n" +
                "    role_id INT NOT NULL,\n" +
                "    created_at DATETIME2 NOT NULL,\n" +
                "    PRIMARY KEY (user_id, role_id)\n" +
                ");";

        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableCompositePK", "testTableCompositePK");

        // 验证表结构
        verifyTableExists("testTableCompositePK", mysqlConfig);
        verifyTableFieldCount("testTableCompositePK", mysqlConfig, 3);

        // 验证复合主键
        verifyTablePrimaryKeys("testTableCompositePK", mysqlConfig, Arrays.asList("user_id", "role_id"));

        logger.info("CREATE TABLE复合主键测试通过");
    }

    // ==================== 通用测试方法 ====================

    /**
     * 执行DDL转换并验证结果（集成测试版本）
     * SQL Server CT 模式下会自动处理 DML 初始化以触发 DDL 检测
     */
    private void testDDLConversion(String sourceDDL, String expectedFieldName) throws Exception {
        // 检查 mapping 是否已经在运行，如果已经在运行就不需要再次启动
        Meta meta = profileComponent.getMapping(mappingId).getMeta();
        if (meta == null || !meta.isRunning()) {
            mappingService.start(mappingId);
            Thread.sleep(2000);
            waitForMetaRunning(metaId, 10000);
        } else {
            logger.debug("Mapping {} 已在运行，跳过启动", mappingId);
        }

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 1. 先执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData = executeInsertDMLToSourceDatabase("ddlTestEmployee", initData, sqlServerConfig);
        waitForDataSync(initData, "ddlTestEmployee", "id", mysqlConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500); // 等待版本号更新

        // 2. 检测是否需要清空表数据（SQL Server 不允许向非空表添加 NOT NULL 列，除非有 DEFAULT）
        boolean isAddNotNullWithoutDefault = sourceDDL.toUpperCase().contains("ADD") && 
                                            sourceDDL.toUpperCase().contains("NOT NULL") && 
                                            !sourceDDL.toUpperCase().contains("DEFAULT");
        if (isAddNotNullWithoutDefault) {
            // 清空表数据，以满足 SQL Server 的语法要求
            clearTableData("ddlTestEmployee", sqlServerConfig);
            clearTableData("ddlTestEmployee", mysqlConfig);
            logger.debug("已清空表数据，以满足 SQL Server 添加 NOT NULL 列的语法要求");
        }

        // 3. 执行 DDL 操作
        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);

        // 4. 执行包含新字段的 INSERT 操作（既触发 DDL 检测，又用于验证数据同步）
        // 对于 ADD COLUMN 操作，需要包含新字段；对于其他操作，使用基础字段即可
        boolean isAddOperation = sourceDDL.toUpperCase().contains("ADD");
        Map<String, Object> insertedData = new HashMap<>();
        insertedData.put("first_name", "Test");
        
        // 如果是 ADD COLUMN 操作，尝试添加新字段的值（根据字段名推断合适的默认值）
        if (isAddOperation) {
            addFieldValueForDDLTest(insertedData, expectedFieldName);
        }
        
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);

        // 5. 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete(expectedFieldName, 10000);

        // 6. 验证 DDL：字段映射和表结构
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

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

        // 7. 验证 DML 数据同步（如果包含新字段）
        if (isAddOperation && insertedData.containsKey(expectedFieldName)) {
            waitForDataSync(insertedData, "ddlTestEmployee", "id", mysqlConfig, 10000);
        }

        logger.info("DDL转换测试通过 - 字段: {}", expectedFieldName);
    }
    
    /**
     * SQL Server CT 模式下执行 DDL 前的初始化（执行 DML 操作以触发 DDL 检测）
     * 这个方法应该在执行 DDL 之前调用，用于初始化表结构快照
     */
    private void prepareForDDLTest() throws Exception {
        // 确保 Mapping 已启动
        Meta meta = profileComponent.getMapping(mappingId).getMeta();
        if (meta == null || !meta.isRunning()) {
            mappingService.start(mappingId);
            Thread.sleep(2000);
            waitForMetaRunning(metaId, 10000);
        }

        // SQL Server CT 模式下，DDL 检测需要 DML 操作来触发
        // 执行一次 DML 操作来初始化表结构快照（插入基础数据并验证同步）
        Map<String, Object> initData = new HashMap<>();
        initData.put("first_name", "Init");
        initData = executeInsertDMLToSourceDatabase("ddlTestEmployee", initData, sqlServerConfig);
        waitForDataSync(initData, "ddlTestEmployee", "id", mysqlConfig, 10000); // 等待并验证初始化数据同步

        Thread.sleep(500); // 等待版本号更新
    }

    /**
     * SQL Server CT 模式下执行 DDL 后的触发操作（执行 DML 操作以触发 DDL 检测）
     * 这个方法应该在执行 DDL 之后调用，用于触发 DDL 检测
     * 
     * @param newFieldName 如果是 ADD COLUMN 操作，传入新字段名；如果是 DROP COLUMN 操作，传入 null（使用其他字段，避免使用被删除的字段）
     * @return 插入的数据（可用于后续验证）
     */
    private Map<String, Object> triggerDDLDetection(String newFieldName) throws Exception {
        Map<String, Object> insertedData = new HashMap<>();
        
        // 如果是 ADD COLUMN 操作，添加基础字段和新字段的值
        if (newFieldName != null && !newFieldName.isEmpty()) {
            insertedData.put("first_name", "Test");
            addFieldValueForDDLTest(insertedData, newFieldName);
        } else {
            // 如果是 DROP COLUMN 操作（newFieldName 为 null），使用其他字段来触发 DDL 检测
            // 注意：对于 DROP COLUMN 操作，被删除的字段不应该出现在 INSERT 语句中
            // 使用 last_name 字段（如果表中有的话），或者使用其他存在的字段
            insertedData.put("last_name", "Test");
        }
        
        insertedData = executeInsertDMLToSourceDatabase("ddlTestEmployee", insertedData, sqlServerConfig);
        return insertedData;
    }

    /**
     * 根据字段名添加合适的测试值（用于 DDL 测试中的 DML 操作）
     */
    private void addFieldValueForDDLTest(Map<String, Object> data, String fieldName) {
        // 根据字段名推断合适的默认值
        String lowerFieldName = fieldName.toLowerCase();
        
        // SQL Server TIMESTAMP (rowversion) 类型不能显式插入值，需要跳过
        // rowversion 字段名通常包含 "rowversion"
        if (lowerFieldName.contains("rowversion")) {
            // TIMESTAMP (rowversion) 类型，不添加值，让 SQL Server 自动生成
            return;
        }
        
        if (lowerFieldName.contains("age") || lowerFieldName.contains("count") || lowerFieldName.contains("num")) {
            data.put(fieldName, 100);
        } else if (lowerFieldName.contains("price") || lowerFieldName.contains("salary") || lowerFieldName.contains("bonus")) {
            data.put(fieldName, 1000.00);
        } else if (lowerFieldName.contains("created") || lowerFieldName.contains("created_at")) {
            data.put(fieldName, java.time.LocalDateTime.now());
        } else if (lowerFieldName.contains("updated") || lowerFieldName.contains("updated_at")) {
            data.put(fieldName, java.time.OffsetDateTime.now());
        } else if (lowerFieldName.contains("event") || lowerFieldName.contains("time")) {
            data.put(fieldName, java.time.LocalDateTime.now());
        } else if (lowerFieldName.contains("is_") || lowerFieldName.contains("active") || lowerFieldName.contains("enabled")) {
            data.put(fieldName, true);
        } else if (lowerFieldName.contains("guid") || (lowerFieldName.contains("id") && lowerFieldName.contains("guid"))) {
            data.put(fieldName, java.util.UUID.randomUUID().toString());
        } else if (lowerFieldName.contains("photo") || lowerFieldName.contains("image")) {
            // IMAGE 类型，使用字节数组
            data.put(fieldName, new byte[]{1, 2, 3, 4});
        } else if (lowerFieldName.contains("description") || lowerFieldName.contains("content") || 
                   lowerFieldName.contains("metadata") || lowerFieldName.contains("text")) {
            data.put(fieldName, "Test " + fieldName);
        } else if (lowerFieldName.contains("name") || lowerFieldName.contains("title")) {
            data.put(fieldName, "Test" + fieldName);
        } else {
            // 默认值：字符串
            data.put(fieldName, "Test");
        }
    }

    /**
     * SQL Server CT 模式下执行 DDL 的包装方法（自动处理 DML 初始化）
     * 这个方法会自动处理 DML 初始化，然后执行 DDL，最后触发 DDL 检测
     * 
     * @param sourceDDL DDL 语句
     * @param newFieldName 如果是 ADD COLUMN 操作，传入新字段名；否则传入 null
     * @return 插入的数据（可用于后续验证）
     */
    private Map<String, Object> executeDDLWithCTSupport(String sourceDDL, String newFieldName) throws Exception {
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测
        return triggerDDLDetection(newFieldName);
    }

    /**
     * 测试DDL DROP操作
     * SQL Server CT 模式下会自动处理 DML 初始化以触发 DDL 检测
     */
    private void testDDLDropOperation(String sourceDDL, String expectedFieldName) throws Exception {
        // 1. 准备 DDL 测试环境（初始化表结构快照）
        prepareForDDLTest();

        // 2. 执行 DDL 操作
        executeDDLToSourceDatabase(sourceDDL, sqlServerConfig);
        
        // 3. 执行 DML 操作来触发 DDL 检测（不包含被删除的字段）
        triggerDDLDetection(null);

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
     * 重置数据库表结构（覆盖基类方法，使用异构数据库的特殊逻辑）
     */
    @Override
    protected void resetDatabaseTableStructure() {
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
     * 执行DDL到源数据库（覆盖基类方法，添加日志）
     */
    @Override
    protected void executeDDLToSourceDatabase(String sql, DatabaseConfig config) throws Exception {
        super.executeDDLToSourceDatabase(sql, config);
        logger.info("-----------execute sql--------- {}", sql);
    }

    // ==================== 辅助验证方法 ====================

    /**
     * 验证字段类型
     */
    private void verifyFieldType(String fieldName, String tableName, DatabaseConfig config, String expectedType) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            String actualType = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            assertNotNull(String.format("未找到字段 %s", fieldName), actualType);
            assertTrue(String.format("字段 %s 的类型应为 %s，但实际是 %s", fieldName, expectedType, actualType),
                    expectedType.equalsIgnoreCase(actualType));
            logger.info("字段类型验证通过: {} 的类型是 {}", fieldName, actualType);
            return null;
        });
    }

    /**
     * 验证字段长度
     */
    private void verifyFieldLength(String fieldName, String tableName, DatabaseConfig config, int expectedLength) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            Integer actualLength = databaseTemplate.queryForObject(sql, Integer.class, tableName, fieldName);
            assertNotNull(String.format("未找到字段 %s 或字段没有长度属性", fieldName), actualLength);
            assertEquals(String.format("字段 %s 的长度应为 %d，但实际是 %d", fieldName, expectedLength, actualLength),
                    Integer.valueOf(expectedLength), actualLength);
            logger.info("字段长度验证通过: {} 的长度是 {}", fieldName, actualLength);
            return null;
        });
    }

    /**
     * 验证字段的COMMENT
     */
    private void verifyFieldComment(String fieldName, String tableName, DatabaseConfig config, String expectedComment) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            String actualComment = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            // MySQL中，如果没有COMMENT，返回空字符串而不是NULL
            String normalizedExpected = expectedComment != null ? expectedComment.trim() : "";
            String normalizedActual = actualComment != null ? actualComment.trim() : "";
            assertTrue(String.format("字段 %s 的COMMENT应为 '%s'，但实际是 '%s'", fieldName, expectedComment, normalizedActual),
                    normalizedExpected.equals(normalizedActual));
            logger.info("字段COMMENT验证通过: {} 的COMMENT是 '{}'", fieldName, normalizedActual);
            return null;
        });
    }

    /**
     * 验证字段是否可空（NULL）
     */
    private void verifyFieldNullable(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
            String isNullable = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            assertNotNull(String.format("未找到字段 %s", fieldName), isNullable);
            assertEquals(String.format("字段 %s 应为NULL（可空），但实际是 %s", fieldName, isNullable),
                    "YES", isNullable.toUpperCase());
            logger.info("字段NULL约束验证通过: {} 是可空的", fieldName);
            return null;
        });
    }

    /**
     * 验证表是否存在
     */
    private void verifyTableExists(String tableName, DatabaseConfig config) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
            Integer count = databaseTemplate.queryForObject(sql, Integer.class, tableName);
            assertTrue(String.format("表 %s 应存在，但未找到", tableName), count != null && count > 0);
            logger.info("表存在验证通过: {}", tableName);
            return null;
        });
    }

    /**
     * 验证表的字段数量
     */
    private void verifyTableFieldCount(String tableName, DatabaseConfig config, int expectedCount) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
            Integer actualCount = databaseTemplate.queryForObject(sql, Integer.class, tableName);
            assertNotNull(String.format("未找到表 %s", tableName), actualCount);
            assertEquals(String.format("表 %s 的字段数量应为 %d，但实际是 %d", tableName, expectedCount, actualCount),
                    Integer.valueOf(expectedCount), actualCount);
            logger.info("表字段数量验证通过: {} 有 {} 个字段", tableName, actualCount);
            return null;
        });
    }

    /**
     * 验证表的主键
     */
    private void verifyTablePrimaryKeys(String tableName, DatabaseConfig config, List<String> expectedKeys) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                    "AND CONSTRAINT_NAME LIKE 'PRIMARY' " +
                    "ORDER BY ORDINAL_POSITION";
            List<String> actualKeys = databaseTemplate.queryForList(sql, String.class, tableName);
            assertNotNull(String.format("未找到表 %s 的主键信息", tableName), actualKeys);
            assertEquals(String.format("表 %s 的主键数量应为 %d，但实际是 %d", tableName, expectedKeys.size(), actualKeys.size()),
                    expectedKeys.size(), actualKeys.size());
            for (int i = 0; i < expectedKeys.size(); i++) {
                String expectedKey = expectedKeys.get(i);
                String actualKey = actualKeys.get(i);
                assertTrue(String.format("表 %s 的主键第 %d 列应为 %s，但实际是 %s", tableName, i + 1, expectedKey, actualKey),
                        expectedKey.equalsIgnoreCase(actualKey));
            }
            logger.info("表主键验证通过: {} 的主键是 {}", tableName, actualKeys);
            return null;
        });
    }

    /**
     * 准备建表测试环境（确保表不存在）
     */
    private void prepareForCreateTableTest(String sourceTable, String targetTable) throws Exception {
        logger.debug("准备建表测试环境，确保表不存在: sourceTable={}, targetTable={}", sourceTable, targetTable);

        // 删除源表和目标表（如果存在）
        forceDropTable(sourceTable, sqlServerConfig);
        forceDropTable(targetTable, sqlServerConfig);
        forceDropTable(sourceTable, mysqlConfig);
        forceDropTable(targetTable, mysqlConfig);

        // 等待删除完成
        Thread.sleep(200);

        logger.debug("建表测试环境准备完成");
    }

    /**
     * 清空表数据（用于满足 SQL Server 添加 NOT NULL 列的语法要求）
     */
    private void clearTableData(String tableName, DatabaseConfig config) {
        try {
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                    new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
            instance.execute(databaseTemplate -> {
                try {
                    String deleteSql = String.format("DELETE FROM %s", tableName);
                    databaseTemplate.execute(deleteSql);
                    logger.debug("已清空表数据: {}", tableName);
                } catch (Exception e) {
                    logger.debug("清空表数据失败: {}", e.getMessage());
                }
                return null;
            });
        } catch (Exception e) {
            logger.debug("清空表数据时出错（可忽略）: {}", e.getMessage());
        }
    }

    /**
     * 强制删除表（忽略不存在的错误）
     */
    private void forceDropTable(String tableName, DatabaseConfig config) {
        try {
            org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                    new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
            instance.execute(databaseTemplate -> {
                try {
                    String dropSql;
                    if (config.getDriverClassName() != null && config.getDriverClassName().contains("sqlserver")) {
                        dropSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", tableName, tableName);
                    } else {
                        dropSql = String.format("DROP TABLE IF EXISTS %s", tableName);
                    }
                    databaseTemplate.execute(dropSql);
                    logger.debug("已删除表: {}", tableName);
                } catch (Exception e) {
                    logger.debug("删除表失败（可能不存在）: {}", e.getMessage());
                }
                return null;
            });
        } catch (Exception e) {
            logger.debug("强制删除表时出错（可忽略）: {}", e.getMessage());
        }
    }

    /**
     * 模拟配置阶段的建表流程：从源表结构创建目标表
     * 这是配置阶段建表的核心逻辑，会触发 COMMENT 转义功能
     */
    private void createTargetTableFromSource(String sourceTable, String targetTable) throws Exception {
        logger.info("开始从源表创建目标表: {} -> {}", sourceTable, targetTable);

        // 确保 connectorType 已设置
        if (sqlServerConfig.getConnectorType() == null) {
            sqlServerConfig.setConnectorType(getConnectorType(sqlServerConfig, true));
        }
        if (mysqlConfig.getConnectorType() == null) {
            mysqlConfig.setConnectorType(getConnectorType(mysqlConfig, false));
        }

        // 1. 连接源和目标数据库
        org.dbsyncer.sdk.connector.ConnectorInstance sourceConnectorInstance = connectorFactory.connect(sqlServerConfig);
        org.dbsyncer.sdk.connector.ConnectorInstance targetConnectorInstance = connectorFactory.connect(mysqlConfig);

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

        // 4. 不同类型数据库：走标准转换流程
        String sourceType = sqlServerConfig.getConnectorType();
        String targetType = mysqlConfig.getConnectorType();
        logger.debug("检测到不同类型数据库（{} -> {}），使用标准转换流程", sourceType, targetType);

        org.dbsyncer.sdk.spi.ConnectorService sourceConnectorService = connectorFactory.getConnectorService(sourceType);
        org.dbsyncer.sdk.spi.ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetType);
        org.dbsyncer.sdk.schema.SchemaResolver sourceSchemaResolver = sourceConnectorService.getSchemaResolver();

        // 创建标准化的 MetaInfo
        org.dbsyncer.sdk.model.MetaInfo standardizedMetaInfo = new org.dbsyncer.sdk.model.MetaInfo();
        standardizedMetaInfo.setTableType(sourceMetaInfo.getTableType());
        standardizedMetaInfo.setSql(sourceMetaInfo.getSql());
        standardizedMetaInfo.setIndexType(sourceMetaInfo.getIndexType());

        // 将源字段转换为标准类型（toStandardType 会自动保留所有元数据属性，包括 COMMENT）
        List<org.dbsyncer.sdk.model.Field> standardizedFields = new ArrayList<>();
        for (org.dbsyncer.sdk.model.Field sourceField : sourceMetaInfo.getColumn()) {
            org.dbsyncer.sdk.model.Field standardField = sourceSchemaResolver.toStandardType(sourceField);
            standardizedFields.add(standardField);
        }
        standardizedMetaInfo.setColumn(standardizedFields);

        // 生成 CREATE TABLE DDL（使用标准化后的 MetaInfo）
        String createTableDDL = targetConnectorService.generateCreateTableDDL(standardizedMetaInfo, targetTable);

        // 5. 执行 CREATE TABLE DDL
        assertNotNull("无法生成 CREATE TABLE DDL", createTableDDL);
        assertFalse("生成的 CREATE TABLE DDL 为空", createTableDDL.trim().isEmpty());

        org.dbsyncer.sdk.config.DDLConfig ddlConfig = new org.dbsyncer.sdk.config.DDLConfig();
        ddlConfig.setSql(createTableDDL);
        org.dbsyncer.common.model.Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig);

        if (result != null && result.error != null && !result.error.trim().isEmpty()) {
            throw new RuntimeException("创建表失败: " + result.error);
        }

        logger.info("成功创建目标表: {}", targetTable);
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
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

    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return SQLServerCTToMySQLDDLSyncIntegrationTest.class;
    }

    @Override
    protected String getSourceConnectorName() {
        return "SQL Server CT源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "MySQL目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "SQL Server CT到MySQL测试Mapping";
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
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        if (isSource) {
            return "SqlServerCT"; // 源使用 CT 模式
        } else {
            return "MySQL"; // 目标是 MySQL
        }
    }

    @Override
    protected String getIncrementStrategy() {
        return "Log"; // CT 模式使用日志监听
    }

    @Override
    protected String getDatabaseType(boolean isSource) {
        if (isSource) {
            return "sqlserver"; // 源是 SQL Server
        } else {
            return "mysql"; // 目标是 MySQL
        }
    }

}

