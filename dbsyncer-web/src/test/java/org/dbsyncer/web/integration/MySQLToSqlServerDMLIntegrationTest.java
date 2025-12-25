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
 * MySQL 到 SQL Server 的 DML 集成测试
 * 专门测试 MySQL -> SQL Server 的 DML 操作，包括 UPSERT 等场景
 * 
 * 测试场景：
 * - UPSERT 操作（MySQL AUTO_INCREMENT -> SQL Server IDENTITY）
 * - 类型转换（VARCHAR -> NVARCHAR, INT AUTO_INCREMENT -> INT IDENTITY）
 * - 数据同步验证
 * 
 * 参考：
 * - BaseDDLIntegrationTest: 提供测试环境初始化和可复用方法
 * - DMLSqlServerIntegrationTest: 参考 SQL Server DML 测试的实现
 * - MySQLToSQLServerDDLSyncIntegrationTest: 参考异构数据库测试的实现
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class MySQLToSqlServerDMLIntegrationTest extends BaseDDLIntegrationTest {

    private static DatabaseConfig mysqlConfig;
    private static DatabaseConfig sqlServerConfig;

    @BeforeClass
    public static void setUpClass() throws Exception {
        logger.info("开始初始化MySQL到SQL Server的DML集成测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 设置基类的sourceConfig和targetConfig
        sourceConfig = mysqlConfig;
        targetConfig = sqlServerConfig;

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(mysqlConfig, sqlServerConfig);

        // 初始化测试环境
        String mysqlInitSql = String.format(
            "DROP TABLE IF EXISTS ddlTestSource;\n" +
            "CREATE TABLE ddlTestSource (\n" +
            "    ID INT AUTO_INCREMENT NOT NULL,\n" +
            "    UserName VARCHAR(50) NOT NULL,\n" +
            "    Age INT NOT NULL,\n" +
            "    Email VARCHAR(100) NULL,\n" +
            "    PRIMARY KEY (UserName)\n" +
            ");");

        String sqlServerInitSql = String.format(
            "IF OBJECT_ID('ddlTestTarget', 'U') IS NOT NULL DROP TABLE ddlTestTarget;\n" +
            "CREATE TABLE ddlTestTarget (\n" +
            "    [ID] INT IDENTITY(1,1) NOT NULL,\n" +
            "    [UserName] NVARCHAR(50) NOT NULL,\n" +
            "    [Age] INT NOT NULL,\n" +
            "    [Email] NVARCHAR(100) NULL,\n" +
            "    PRIMARY KEY ([UserName])\n" +
            ");");

        testDatabaseManager.initializeTestEnvironment(mysqlInitSql, sqlServerInitSql);

        logger.info("MySQL到SQL Server的DML集成测试环境初始化完成");
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
        Properties props = new Properties();
        try (InputStream input = MySQLToSqlServerDMLIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                mysqlConfig = createDefaultMySQLConfig();
                sqlServerConfig = createDefaultSQLServerConfig();
                return;
            }
            props.load(input);
        }

        // 创建源数据库配置(MySQL)
        mysqlConfig = new DatabaseConfig();
        mysqlConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/source_db"));
        mysqlConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        mysqlConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        mysqlConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));

        // 创建目标数据库配置(SQL Server)
        sqlServerConfig = new DatabaseConfig();
        sqlServerConfig.setUrl(props.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=target_db;encrypt=false;trustServerCertificate=true"));
        sqlServerConfig.setUsername(props.getProperty("test.db.sqlserver.username", "sa"));
        sqlServerConfig.setPassword(props.getProperty("test.db.sqlserver.password", "123456"));
        sqlServerConfig.setDriverClassName(props.getProperty("test.db.sqlserver.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理MySQL到SQL Server的DML集成测试环境");

        try {
            String sourceCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "mysql", MySQLToSqlServerDMLIntegrationTest.class);
            String targetCleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "sqlserver", MySQLToSqlServerDMLIntegrationTest.class);
            testDatabaseManager.cleanupTestEnvironment(sourceCleanupSql, targetCleanupSql);

            logger.info("MySQL到SQL Server的DML集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        cleanupResidualTestMappings();

        // 创建Connector
        sourceConnectorId = createConnector(getSourceConnectorName(), sourceConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), targetConfig, false);

        // 先创建表结构
        resetDatabaseTableStructure();

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("MySQL到SQL Server的DML集成测试用例环境初始化完成");
    }

    /**
     * 覆盖 resetDatabaseTableStructure 方法，创建正确的表结构
     */
    @Override
    protected void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            String testSourceTable = getSourceTableName();
            String testTargetTable = getTargetTableName();
            
            // MySQL 表（AUTO_INCREMENT 列必须是主键或唯一键）
            String dropSourceSql = String.format("DROP TABLE IF EXISTS %s", testSourceTable);
            String createSourceTableDDL = String.format(
                "CREATE TABLE %s (\n" +
                "    ID INT AUTO_INCREMENT NOT NULL,\n" +
                "    UserName VARCHAR(50) NOT NULL,\n" +
                "    Age INT NOT NULL,\n" +
                "    Email VARCHAR(100) NULL,\n" +
                "    PRIMARY KEY (ID),\n" +
                "    UNIQUE KEY (UserName)\n" +
                ")", testSourceTable);
            
            // SQL Server 表
            String dropTargetSql = String.format("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", testTargetTable, testTargetTable);
            String createTargetTableDDL = String.format(
                "CREATE TABLE %s (\n" +
                "    [ID] INT IDENTITY(1,1) NOT NULL,\n" +
                "    [UserName] NVARCHAR(50) NOT NULL,\n" +
                "    [Age] INT NOT NULL,\n" +
                "    [Email] NVARCHAR(100) NULL,\n" +
                "    PRIMARY KEY ([UserName])\n" +
                ")", testTargetTable);
            
            executeDDLToSourceDatabase(dropSourceSql, sourceConfig);
            executeDDLToSourceDatabase(dropTargetSql, targetConfig);
            executeDDLToSourceDatabase(createSourceTableDDL, sourceConfig);
            executeDDLToSourceDatabase(createTargetTableDDL, targetConfig);
            
            logger.debug("测试数据库表结构重置完成");
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    @After
    public void tearDown() {
        try {
            if (mappingId != null) {
                try {
                    mappingService.stop(mappingId);
                    Thread.sleep(500);
                } catch (Exception e) {
                    logger.debug("停止Mapping时出错: {}", e.getMessage());
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
     * 测试 INSERT 操作 - MySQL 到 SQL Server 全量同步插入数据
     */
    @Test
    public void testInsert_FullSync() throws Exception {
        logger.info("开始测试 INSERT 操作 - MySQL 到 SQL Server 全量同步插入数据");

        String testSourceTable = getSourceTableName();
        String testTargetTable = getTargetTableName();

        // 1. 重置 TableGroup 完成状态
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 2. 配置 Mapping（不启用 forceUpdate）
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
        
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, sourceData.get("UserName"), 
            sourceData.get("Age"), sourceData.get("Email"));
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(insertSourceSql);
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

        logger.info("INSERT MySQL到SQL Server全量同步测试通过");
    }

    // ==================== UPDATE 测试场景 ====================

    /**
     * 测试 UPDATE 操作 - MySQL 到 SQL Server 更新已存在的数据
     */
    @Test
    public void testUpdate_ExistingData() throws Exception {
        logger.info("开始测试 UPDATE 操作 - MySQL 到 SQL Server 更新已存在的数据");

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
        
        // MySQL 源表插入
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, userName, 25, "old@example.com");
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(insertSourceSql);
            return null;
        });

        // SQL Server 目标表插入
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

        // 4. 启动全量同步（第一次同步）
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

        logger.info("UPDATE MySQL到SQL Server操作测试通过");
    }

    // ==================== DELETE 测试场景 ====================

    /**
     * 测试 DELETE 操作 - MySQL 到 SQL Server 删除数据
     */
    @Test
    public void testDelete_RemoveData() throws Exception {
        logger.info("开始测试 DELETE 操作 - MySQL 到 SQL Server 删除数据");

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

        // 3. 先在源表插入数据
        Integer testId = 400;
        String userName = "DeleteUser";
        
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, userName, 30, "delete@example.com");
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(insertSourceSql);
            return null;
        });

        // 4. 启动全量同步（插入数据）
        mappingService.start(mappingId);
        Thread.sleep(2000);

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

        // 7. 再次启动全量同步（应该触发 DELETE）
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);
        mappingService.start(mappingId);
        Thread.sleep(3000);

        // 8. 验证删除结果
        Map<String, Object> targetDataAfterDelete = queryTableDataByPrimaryKey(testTargetTable, "UserName", userName, targetConfig);
        assertNull("目标表中数据应该被删除", targetDataAfterDelete);

        logger.info("DELETE MySQL到SQL Server操作测试通过");
    }

    // ==================== UPSERT 测试场景 ====================

    /**
     * 测试 UPSERT 操作 - MySQL AUTO_INCREMENT 到 SQL Server IDENTITY
     */
    @Test
    public void testUpsert_AutoIncrementToIdentity() throws Exception {
        logger.info("开始测试 UPSERT 操作 - MySQL AUTO_INCREMENT 到 SQL Server IDENTITY");

        String testSourceTable = getSourceTableName();
        String testTargetTable = getTargetTableName();

        // 1. 重置 TableGroup 完成状态
        List<org.dbsyncer.parser.model.TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        org.dbsyncer.parser.model.TableGroup tableGroup = tableGroups.get(0);
        tableGroup.setFullCompleted(false);
        tableGroup.setCursors(null);
        profileComponent.editConfigModel(tableGroup);

        // 2. 启用 forceUpdate
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

        // 3. 在源表和目标表同时插入数据
        Integer testId = 100;
        
        // MySQL 源表插入
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("ID", testId);
        sourceData.put("UserName", "TestUser1");
        sourceData.put("Age", 25);
        sourceData.put("Email", "source@example.com");
        
        String insertSourceSql = String.format(
            "INSERT INTO %s (ID, UserName, Age, Email) VALUES (%d, '%s', %d, '%s')",
            testSourceTable, testId, sourceData.get("UserName"), 
            sourceData.get("Age"), sourceData.get("Email"));
        
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance sourceInstance = 
            new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(sourceConfig);
        sourceInstance.execute(databaseTemplate -> {
            databaseTemplate.execute(insertSourceSql);
            return null;
        });
        logger.info("源表已插入数据，ID: {}, Age: {}, Email: {}", testId, sourceData.get("Age"), sourceData.get("Email"));

        // SQL Server 目标表插入
        Map<String, Object> targetData = new HashMap<>();
        targetData.put("ID", testId);
        targetData.put("UserName", "TestUser1");
        targetData.put("Age", 30);
        targetData.put("Email", "target@example.com");
        
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

        // 4. 启动全量同步
        try {
            mappingService.start(mappingId);
            Thread.sleep(3000);
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null && (errorMsg.contains("无法更新标识列") || errorMsg.contains("Cannot update identity column"))) {
                fail("UPSERT 操作不应该尝试更新标识列: " + errorMsg);
            }
            throw e;
        }

        // 5. 验证结果
        Map<String, Object> sourceDataAfterSync = queryTableDataByPrimaryKey(testSourceTable, "UserName", "TestUser1", sourceConfig);
        Map<String, Object> targetDataAfterSync = queryTableDataByPrimaryKey(testTargetTable, "UserName", "TestUser1", targetConfig);

        assertNotNull("源表中应该找到数据", sourceDataAfterSync);
        assertNotNull("目标表中应该找到数据", targetDataAfterSync);

        assertEquals("源表中的ID不应该被更新", testId, sourceDataAfterSync.get("ID"));
        assertEquals("目标表中的ID不应该被更新", testId, targetDataAfterSync.get("ID"));
        assertEquals("Age字段应该通过UPSERT更新", sourceData.get("Age"), targetDataAfterSync.get("Age"));
        assertEquals("Email字段应该通过UPSERT更新", sourceData.get("Email"), targetDataAfterSync.get("Email"));

        logger.info("UPSERT MySQL到SQL Server测试通过");
    }

    // ==================== 辅助方法 ====================

    private Map<String, Object> queryTableDataByPrimaryKey(String tableName, String primaryKeyColumn, Object primaryKeyValue, DatabaseConfig config) throws Exception {
        String whereCondition = primaryKeyColumn + " = " + (primaryKeyValue instanceof String ? "'" + primaryKeyValue + "'" : primaryKeyValue);
        String sql = String.format("SELECT * FROM %s WHERE %s", tableName, whereCondition);
        return queryTableData(sql, config);
    }

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
        return MySQLToSqlServerDMLIntegrationTest.class;
    }

    @Override
    protected String getSourceConnectorName() {
        return "MySQL到SQL Server DML源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "MySQL到SQL Server DML目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "MySQL到SQL Server DML测试Mapping";
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
        fieldMappingList.add("ID|ID");
        fieldMappingList.add("UserName|UserName");
        fieldMappingList.add("Age|Age");
        fieldMappingList.add("Email|Email");
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        return isSource ? "MySQL" : "SqlServer";
    }

    @Override
    protected String getIncrementStrategy() {
        return "Timing";
    }

    @Override
    protected String createMapping() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", getMappingName());
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "full");
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);

        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "full");
        editParams.put("incrementStrategy", "Timing");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

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
        return isSource ? "mysql" : "sqlserver";
    }
}

