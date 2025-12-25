package org.dbsyncer.web.integration;

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
 * SQL Server DML 集成测试
 * 专门测试 UPSERT 操作中标识列更新的问题
 * 
 * 测试场景：
 * - UPSERT 操作时，标识列不应该在 UPDATE 子句中被更新
 * - 即使开启了 IDENTITY_INSERT，MERGE 语句的 UPDATE 部分仍然不能更新标识列
 * - 验证修复后的代码能够正确处理标识列，避免 "无法更新标识列 'ID'" 错误
 * 
 * 参考：
 * - BaseDDLIntegrationTest: 提供测试环境初始化和可复用方法
 * - DDLSqlServerCTIntegrationTest: 参考 SQL Server 测试的初始化方式
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class DMLSqlServerIntegrationTest extends BaseDDLIntegrationTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("开始初始化SQL Server DML集成测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境（使用按数据库类型分类的脚本）
        String initSql = loadSqlScriptByDatabaseTypeStatic("reset-test-table", "sqlserver", DMLSqlServerIntegrationTest.class);
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("SQL Server DML集成测试环境初始化完成");
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
        Properties props = new Properties();
        try (InputStream input = DMLSqlServerIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
        logger.info("开始清理SQL Server DML集成测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            String cleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "sqlserver", DMLSqlServerIntegrationTest.class);
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("SQL Server DML集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 先清理可能残留的测试 mapping（防止上一个测试清理失败导致残留）
        cleanupResidualTestMappings();

        // 创建Connector
        sourceConnectorId = createConnector(getSourceConnectorName(), sourceConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), targetConfig, false);

        // 先创建表结构（必须在 createMapping 之前，因为 createMapping 需要表结构来匹配字段映射）
        resetDatabaseTableStructure();

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("SQL Server DML集成测试用例环境初始化完成");
    }

    /**
     * 覆盖 resetDatabaseTableStructure 方法，创建包含 IDENTITY 列的正确表结构
     * 表结构必须与 getInitialFieldMappings() 返回的字段映射匹配
     */
    @Override
    protected void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构（使用 IDENTITY 列的表结构）");
        try {
            String testSourceTable = getSourceTableName();
            String testTargetTable = getTargetTableName();
            
            // 删除表
            String dropSourceSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", testSourceTable, testSourceTable);
            String dropTargetSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", testTargetTable, testTargetTable);
            executeDDLToSourceDatabase(dropSourceSql, sourceConfig);
            executeDDLToSourceDatabase(dropTargetSql, targetConfig);
            
            // 创建包含 IDENTITY 列的表（字段必须与 getInitialFieldMappings() 匹配）
            String createSourceTableDDL = String.format(
                "CREATE TABLE %s (\n" +
                "    [ID] INT IDENTITY(1,1) NOT NULL,\n" +
                "    [UserName] NVARCHAR(50) NOT NULL,\n" +
                "    [Age] INT NOT NULL,\n" +
                "    [Email] NVARCHAR(100) NULL,\n" +
                "    PRIMARY KEY ([UserName])\n" +
                ")", testSourceTable);
            
            String createTargetTableDDL = String.format(
                "CREATE TABLE %s (\n" +
                "    [ID] INT IDENTITY(1,1) NOT NULL,\n" +
                "    [UserName] NVARCHAR(50) NOT NULL,\n" +
                "    [Age] INT NOT NULL,\n" +
                "    [Email] NVARCHAR(100) NULL,\n" +
                "    PRIMARY KEY ([UserName])\n" +
                ")", testTargetTable);
            
            executeDDLToSourceDatabase(createSourceTableDDL, sourceConfig);
            executeDDLToSourceDatabase(createTargetTableDDL, targetConfig);
            
            logger.debug("测试数据库表结构重置完成（包含 IDENTITY 列）");
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    @After
    public void tearDown() {
        // 停止并清理Mapping（必须先停止，否则Connector无法删除）
        try {
            if (mappingId != null) {
                try {
                    mappingService.stop(mappingId);
                    // 等待一下，确保完全停止
                    Thread.sleep(500);
                } catch (Exception e) {
                    // 可能已经停止，忽略
                    logger.debug("停止Mapping时出错（可能已经停止）: {}", e.getMessage());
                }
                try {
                    mappingService.remove(mappingId);
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
                } catch (Exception e) {
                    logger.warn("删除源Connector失败: {}", e.getMessage());
                }
            }
            if (targetConnectorId != null) {
                try {
                    connectorService.remove(targetConnectorId);
                } catch (Exception e) {
                    logger.warn("删除目标Connector失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warn("清理Connector失败", e);
        }
    }

    // ==================== INSERT 测试场景 ====================

    /**
     * 测试 INSERT 操作 - 全量同步插入数据
     */
    @Test
    public void testInsert_FullSync() throws Exception {
        logger.info("开始测试 INSERT 操作 - 全量同步插入数据");

        String testSourceTable = getSourceTableName();
        String testTargetTable = getTargetTableName();

        // 1. 重置 TableGroup 完成状态
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 2. 配置 Mapping（不启用 forceUpdate，使用普通 INSERT）
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "full");
        editParams.put("incrementStrategy", "Timing");
        editParams.put("forceUpdate", "false");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 3. 在源表插入数据
        Integer testId = 200;
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("ID", testId);
        sourceData.put("UserName", "InsertUser");
        sourceData.put("Age", 28);
        sourceData.put("Email", "insert@example.com");
        
        String sourceIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testSourceTable);
        String sourceIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testSourceTable);
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, sourceData.get("UserName"), 
            sourceData.get("Age"), sourceData.get("Email"));
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(sourceIdentityInsertOn);
            try {
                databaseTemplate.execute(insertSourceSql);
            } finally {
                databaseTemplate.execute(sourceIdentityInsertOff);
            }
            return null;
        });
        logger.info("源表已插入数据，ID: {}, UserName: {}", testId, sourceData.get("UserName"));

        // 4. 启动全量同步
        mappingService.start(mappingId);
        Thread.sleep(3000);

        // 5. 验证数据同步
        Map<String, Object> targetData = queryTableDataByPrimaryKey(testTargetTable, "UserName", "InsertUser", targetConfig);
        assertNotNull("目标表中应该找到数据", targetData);
        assertEquals("ID应该同步", testId, targetData.get("ID"));
        assertEquals("Age应该同步", sourceData.get("Age"), targetData.get("Age"));
        assertEquals("Email应该同步", sourceData.get("Email"), targetData.get("Email"));

        logger.info("INSERT 全量同步测试通过");
    }

    // ==================== UPDATE 测试场景 ====================

    /**
     * 测试 UPDATE 操作 - 更新已存在的数据
     */
    @Test
    public void testUpdate_ExistingData() throws Exception {
        logger.info("开始测试 UPDATE 操作 - 更新已存在的数据");

        String testSourceTable = getSourceTableName();
        String testTargetTable = getTargetTableName();

        // 1. 重置 TableGroup 完成状态
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 2. 配置 Mapping
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "full");
        editParams.put("incrementStrategy", "Timing");
        editParams.put("forceUpdate", "false");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 3. 先在源表和目标表插入初始数据
        Integer testId = 300;
        String userName = "UpdateUser";
        
        // 源表插入
        String sourceIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testSourceTable);
        String sourceIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testSourceTable);
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, userName, 25, "old@example.com");
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(sourceIdentityInsertOn);
            try {
                databaseTemplate.execute(insertSourceSql);
            } finally {
                databaseTemplate.execute(sourceIdentityInsertOff);
            }
            return null;
        });

        // 目标表插入（相同主键）
        String targetIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testTargetTable);
        String targetIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testTargetTable);
        String insertTargetSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testTargetTable, testId, userName, 25, "old@example.com");
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance targetInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(targetConfig);
        targetInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(targetIdentityInsertOn);
            try {
                databaseTemplate.execute(insertTargetSql);
            } finally {
                databaseTemplate.execute(targetIdentityInsertOff);
            }
            return null;
        });

        // 4. 启动全量同步（第一次同步，插入数据）
        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 5. 更新源表数据
        String updateSourceSql = String.format(
            "UPDATE %s SET Age = %d, Email = '%s' WHERE UserName = '%s'",
            testSourceTable, 35, "new@example.com", userName);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(updateSourceSql);
            return null;
        });
        logger.info("源表已更新数据，UserName: {}, 新Age: 35, 新Email: new@example.com", userName);

        // 6. 再次启动全量同步（应该触发 UPDATE）
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);
        mappingService.start(mappingId);
        Thread.sleep(3000);

        // 7. 验证更新结果
        Map<String, Object> targetData = queryTableDataByPrimaryKey(testTargetTable, "UserName", userName, targetConfig);
        assertNotNull("目标表中应该找到数据", targetData);
        assertEquals("Age应该被更新", 35, targetData.get("Age"));
        assertEquals("Email应该被更新", "new@example.com", targetData.get("Email"));

        logger.info("UPDATE 操作测试通过");
    }

    // ==================== DELETE 测试场景 ====================

    /**
     * 测试 DELETE 操作 - 删除数据
     */
    @Test
    public void testDelete_RemoveData() throws Exception {
        logger.info("开始测试 DELETE 操作 - 删除数据");

        String testSourceTable = getSourceTableName();
        String testTargetTable = getTargetTableName();

        // 1. 重置 TableGroup 完成状态
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 2. 配置 Mapping 为增量同步模式（Log），因为全量同步不会检测 DELETE 操作
        // Log 模式使用 CT（Change Tracking）实时监听变更，比 Timing 模式更快更准确
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment");
        editParams.put("incrementStrategy", "Log"); // 使用 Log 模式（CT）实时监听 DELETE
        editParams.put("forceUpdate", "false");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 3. 先启动增量同步（让监听器开始工作，准备捕获后续的 INSERT 和 DELETE 操作）
        mappingService.start(mappingId);
        Thread.sleep(2000); // 等待增量同步监听器就绪

        // 4. 在源表插入数据（此时会被实时监听捕获）
        Integer testId = 400;
        String userName = "DeleteUser";
        
        String sourceIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testSourceTable);
        String sourceIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testSourceTable);
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, userName, 30, "delete@example.com");
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(sourceIdentityInsertOn);
            try {
                databaseTemplate.execute(insertSourceSql);
            } finally {
                databaseTemplate.execute(sourceIdentityInsertOff);
            }
            return null;
        });
        
        Thread.sleep(2000); // 等待增量同步处理 INSERT（Log 模式实时监听，等待时间更短）

        // 5. 验证数据已同步到目标表
        Map<String, Object> targetDataBeforeDelete = queryTableDataByPrimaryKey(testTargetTable, "UserName", userName, targetConfig);
        assertNotNull("删除前目标表中应该找到数据", targetDataBeforeDelete);

        // 6. 从源表删除数据
        String deleteSourceSql = String.format("DELETE FROM %s WHERE UserName = '%s'", testSourceTable, userName);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(deleteSourceSql);
            return null;
        });
        logger.info("源表已删除数据，UserName: {}", userName);

        // 7. 等待增量同步处理 DELETE（Log 模式实时监听变更，使用轮询方式等待）
        long startTime = System.currentTimeMillis();
        long timeoutMs = 10000; // 10秒超时
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            Map<String, Object> checkData = queryTableDataByPrimaryKey(testTargetTable, "UserName", userName, targetConfig);
            if (checkData.isEmpty()) {
                logger.info("目标表中数据已被删除，等待时间: {}ms", System.currentTimeMillis() - startTime);
                break;
            }
            Thread.sleep(300); // 每300ms检查一次
        }
        
        // 8. 验证删除结果
        Map<String, Object> targetDataAfterDelete = queryTableDataByPrimaryKey(testTargetTable, "UserName", userName, targetConfig);
        assertTrue("目标表中数据应该被删除（查询结果应为空）", targetDataAfterDelete.isEmpty());

        logger.info("DELETE 操作测试通过");
    }

    // ==================== UPSERT 标识列更新测试场景 ====================

    /**
     * 测试 UPSERT 操作 - 标识列不应该在 UPDATE 子句中被更新
     * 
     * 测试步骤：
     * 1. 创建包含 IDENTITY 列的表（ID 为标识列，UserName 为主键）
     * 2. 创建 Mapping 并启用 forceUpdate（覆盖模式）
     * 3. 启动同步
     * 4. 插入第一条数据到源表（会同步到目标表）
     * 5. 再次插入相同主键的数据到源表（会触发 UPSERT，因为目标表已存在该主键）
     * 6. 验证：
     *    - UPSERT 操作成功（不应该抛出 "无法更新标识列 'ID'" 错误）
     *    - ID 字段没有被更新（标识列不应该被更新）
     *    - 其他字段正常更新
     * 
     * 这是修复的核心验证点：通过实际同步流程触发 UPSERT，测试 buildBatchUpsertSql 方法
     * 生成的 MERGE 语句的 UPDATE 子句不包含标识列
     */
    @Test
    public void testUpsert_IdentityColumnShouldNotBeUpdated() throws Exception {
        logger.info("开始测试 UPSERT 操作 - 标识列不应该在 UPDATE 子句中被更新");

        // 直接使用默认表名，表结构已在 setUp 中通过 resetDatabaseTableStructure() 创建
        String testSourceTable = getSourceTableName(); // "ddlTestSource"
        String testTargetTable = getTargetTableName(); // "ddlTestTarget"

        // 1. 重置 TableGroup 完成状态，确保全量同步会处理数据
        // 注意：字段映射已在 createMapping() 中通过 getInitialFieldMappings() 正确设置
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        
        // 重置完成状态，确保全量同步会处理数据
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 4. 启用 forceUpdate（覆盖模式），这样 INSERT 操作会转换为 UPSERT
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "full");
        editParams.put("incrementStrategy", "Timing");
        editParams.put("forceUpdate", "true");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 5. 在源表和目标表同时插入一条ID相同但数据不同的数据
        // 关键点：数据中包含 ID 字段，但 buildBatchUpsertSql 生成的 MERGE 语句的 UPDATE 子句不应该包含 ID
        Integer testId = 100; // 使用一个固定的ID值
        
        // 在源表插入数据（包含ID字段）
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("ID", testId);
        sourceData.put("UserName", "TestUser1");
        sourceData.put("Age", 25);
        sourceData.put("Email", "source@example.com");
        
        // 使用 IDENTITY_INSERT 在源表插入数据
        String sourceIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testSourceTable);
        String sourceIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testSourceTable);
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, sourceData.get("UserName"), 
            sourceData.get("Age"), sourceData.get("Email"));
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(sourceIdentityInsertOn);
            try {
                databaseTemplate.execute(insertSourceSql);
            } finally {
                databaseTemplate.execute(sourceIdentityInsertOff);
            }
            return null;
        });
        logger.info("源表已插入数据，ID: {}, Age: {}, Email: {}", testId, sourceData.get("Age"), sourceData.get("Email"));

        // 在目标表插入数据（ID相同，但数据不同）
        Map<String, Object> targetData = new HashMap<>();
        targetData.put("ID", testId); // 相同的ID
        targetData.put("UserName", "TestUser1");
        targetData.put("Age", 30); // 不同的年龄
        targetData.put("Email", "target@example.com"); // 不同的邮箱
        
        // 使用 IDENTITY_INSERT 在目标表插入数据
        String targetIdentityInsertOn = String.format("SET IDENTITY_INSERT %s ON", testTargetTable);
        String targetIdentityInsertOff = String.format("SET IDENTITY_INSERT %s OFF", testTargetTable);
        String insertTargetSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testTargetTable, testId, targetData.get("UserName"), 
            targetData.get("Age"), targetData.get("Email"));
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance targetInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(targetConfig);
        targetInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(targetIdentityInsertOn);
            try {
                databaseTemplate.execute(insertTargetSql);
            } finally {
                databaseTemplate.execute(targetIdentityInsertOff);
            }
            return null;
        });
        logger.info("目标表已插入数据，ID: {}, Age: {}, Email: {}", testId, targetData.get("Age"), targetData.get("Email"));

        // 6. 启动全量同步（会触发 UPSERT，因为目标表已存在该主键且 forceUpdate=true）
        // 这会调用 buildBatchUpsertSql 方法生成 MERGE 语句
        // 修复前：如果 UPDATE 子句包含 ID 字段，会抛出 "无法更新标识列 'ID'" 错误
        // 修复后：UPDATE 子句不包含 ID 字段，应该成功执行
        try {
            mappingService.start(mappingId);
            
            // 等待全量同步完成（全量同步完成后 Meta 状态会变为 0，这是正常的）
            Thread.sleep(3000);
        } catch (Exception e) {
            // 如果抛出 "无法更新标识列 'ID'" 错误，说明修复失败
            String errorMsg = e.getMessage();
            if (errorMsg != null && (errorMsg.contains("无法更新标识列") || errorMsg.contains("Cannot update identity column"))) {
                fail("UPSERT 操作不应该尝试更新标识列，但抛出了错误: " + errorMsg);
            }
            throw e;
        }

        // 7. 验证结果
        Map<String, Object> sourceDataAfterSync = queryTableDataByPrimaryKey(testSourceTable, "UserName", "TestUser1", sourceConfig);
        Map<String, Object> targetDataAfterSync = queryTableDataByPrimaryKey(testTargetTable, "UserName", "TestUser1", targetConfig);

        assertNotNull("源表中应该找到数据", sourceDataAfterSync);
        assertNotNull("目标表中应该找到数据", targetDataAfterSync);

        // 验证 ID 没有被更新（标识列不应该被更新）
        assertEquals("源表中的ID不应该被更新", testId, sourceDataAfterSync.get("ID"));
        assertEquals("目标表中的ID不应该被更新", testId, targetDataAfterSync.get("ID"));

        // 验证其他字段通过 UPSERT 正常更新（目标表应该使用源表的数据）
        assertEquals("Age字段应该通过UPSERT更新为源表的值", sourceData.get("Age"), targetDataAfterSync.get("Age"));
        assertEquals("Email字段应该通过UPSERT更新为源表的值", sourceData.get("Email"), targetDataAfterSync.get("Email"));

        logger.info("UPSERT 标识列更新测试通过（标识列没有被更新，其他字段正常更新，符合修复后的逻辑）");
    }

    // ==================== 辅助方法 ====================

    /**
     * 根据主键查询表数据
     */
    private Map<String, Object> queryTableDataByPrimaryKey(String tableName, String primaryKeyColumn, Object primaryKeyValue, DatabaseConfig config) throws Exception {
        String whereCondition = primaryKeyColumn + " = " + (primaryKeyValue instanceof String ? "'" + primaryKeyValue + "'" : primaryKeyValue);
        String sql = String.format("SELECT * FROM %s WHERE %s", tableName, whereCondition);
        return queryTableData(sql, config);
    }

    /**
     * 查询表数据（返回第一行数据）
     */
    private Map<String, Object> queryTableData(String sql, DatabaseConfig config) throws Exception {
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        return instance.execute(databaseTemplate -> {
            return databaseTemplate.query(sql, (java.sql.ResultSet rs) -> {
                Map<String, Object> data = new HashMap<>();
                if (rs.next()) {
                    java.sql.ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        data.put(columnName, value);
                    }
                }
                return data;
            });
        });
    }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return DMLSqlServerIntegrationTest.class;
    }

    @Override
    protected String getSourceConnectorName() {
        return "SQL Server DML源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "SQL Server DML目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "SQL Server DML测试Mapping";
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
        // 返回测试所需的字段映射（包含 IDENTITY 列）
        List<String> fieldMappingList = new ArrayList<>();
        fieldMappingList.add("ID|ID");
        fieldMappingList.add("UserName|UserName");
        fieldMappingList.add("Age|Age");
        fieldMappingList.add("Email|Email");
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        return "SqlServerCT"; // 使用 CT 模式以支持 Log 增量同步（实时监听 DELETE）
    }

    @Override
    protected String getIncrementStrategy() {
        // 注意：对于 DML 测试，特别是测试 UPSERT，应该使用全量同步模式
        // 但 BaseDDLIntegrationTest.createMapping() 硬编码了 "increment" 模式
        // 这里返回一个值，但实际会在 createMapping 中覆盖为全量模式
        // 对于 DELETE 测试，会使用 Log 模式（实时监听）
        return "Log"; // Log 模式用于实时监听变更（CT/binlog）
    }

    /**
     * 重写 createMapping 方法，使用全量同步模式
     * 全量模式可以立即同步数据，不需要等待定时任务，更适合 DML 测试
     */
    @Override
    protected String createMapping() throws Exception {
        // 先创建Mapping（不包含tableGroups）
        Map<String, String> params = new HashMap<>();
        params.put("name", getMappingName());
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "full"); // 使用全量同步模式，可以立即同步数据
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);

        // 创建后需要编辑一次以正确设置配置
        // 注意：即使使用全量模式，验证逻辑也要求 incrementStrategy，传递一个占位值
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "full");
        editParams.put("incrementStrategy", "Timing"); // 全量模式不需要，但验证逻辑要求，传递占位值
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 然后使用tableGroupService.add()创建TableGroup
        Map<String, String> tableGroupParams = new HashMap<>();
        tableGroupParams.put("mappingId", mappingId);
        tableGroupParams.put("sourceTable", getSourceTableName());
        tableGroupParams.put("targetTable", getTargetTableName());
        tableGroupParams.put("fieldMappings", String.join(",", getInitialFieldMappings()));
        tableGroupService.add(tableGroupParams);

        return mappingId;
    }

    @Override
    protected String getDatabaseType(boolean isSource) {
        return "sqlserver"; // SQL Server 数据库类型
    }
}

