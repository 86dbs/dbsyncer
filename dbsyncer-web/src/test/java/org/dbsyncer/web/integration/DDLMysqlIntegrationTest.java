package org.dbsyncer.web.integration;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.web.Application;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
 * MySQL到MySQL的DDL同步集成测试
 * 全面测试MySQL之间DDL同步的端到端功能，包括解析、转换和执行
 * 覆盖场景：
 * - ADD COLUMN: 基础添加、带位置（FIRST/AFTER）、带默认值、带约束
 * - DROP COLUMN: 删除字段
 * - MODIFY COLUMN: 修改类型、修改长度、修改约束
 * - CHANGE COLUMN: 重命名字段、重命名并修改类型
 * - 异常处理
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
public class DDLMysqlIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DDLMysqlIntegrationTest.class);

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
    public static void setUpClass() throws IOException {
        logger.info("开始初始化DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfig();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境（使用按数据库类型分类的脚本）
        String initSql = loadSqlScriptByDatabaseType("reset-test-table", sourceConfig);
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("DDL同步集成测试环境初始化完成");
    }

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理DDL同步集成测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            String cleanupSql = loadSqlScriptByDatabaseType("cleanup-test-data", sourceConfig);
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("DDL同步集成测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        // 确保每个测试开始时数据库表结构是初始状态
        resetDatabaseTableStructure();

        // 创建Connector
        sourceConnectorId = createConnector("MySQL源连接器", sourceConfig);
        targetConnectorId = createConnector("MySQL目标连接器", targetConfig);

        // 创建Mapping和TableGroup
        mappingId = createMapping();
        metaId = profileComponent.getMapping(mappingId).getMetaId();

        logger.info("DDL同步集成测试用例环境初始化完成");
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

    /**
     * 重置数据库表结构到初始状态
     * 确保每个测试开始时表结构是干净的初始状态
     */
    private void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            // 先强制删除表（确保完全清理，包括所有字段）
            // 注意：由于源数据库和目标数据库可能是同一个，需要确保两个表都被删除
            forceDropTable("ddlTestSource", sourceConfig);
            forceDropTable("ddlTestTarget", sourceConfig);
            forceDropTable("ddlTestSource", targetConfig);
            forceDropTable("ddlTestTarget", targetConfig);
            
            // 等待一小段时间，确保删除操作完成
            Thread.sleep(100);
            
            // 使用按数据库类型分类的脚本重建表
            String resetSql = loadSqlScriptByDatabaseType("reset-test-table", sourceConfig);
            if (resetSql != null && !resetSql.trim().isEmpty()) {
                testDatabaseManager.resetTableStructure(resetSql, resetSql);
                logger.debug("测试数据库表结构重置完成");
                
                // 再次等待，确保表创建完成
                Thread.sleep(100);
                
                // 验证表结构确实被重置（检查表是否存在且只有初始字段）
                verifyTableStructureReset();
            } else {
                logger.warn("重置SQL脚本为空，无法重置表结构");
            }
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
            // 重置失败时，尝试强制删除并重建表
            try {
                forceResetTable();
            } catch (Exception ex) {
                logger.error("强制重置表结构也失败", ex);
            }
        }
    }

    /**
     * 强制删除表（忽略不存在的错误）
     */
    private void forceDropTable(String tableName, DatabaseConfig config) {
        try {
            DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
            instance.execute(databaseTemplate -> {
                try {
                    String dropSql = String.format("DROP TABLE IF EXISTS %s", tableName);
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
     * 验证表结构是否已正确重置
     */
    private void verifyTableStructureReset() {
        try {
            DatabaseConnectorInstance instance = new DatabaseConnectorInstance(sourceConfig);
            instance.execute(databaseTemplate -> {
                // 检查源表是否存在
                String checkSourceTableSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ddlTestSource'";
                Integer sourceTableCount = databaseTemplate.queryForObject(checkSourceTableSql, Integer.class);
                if (sourceTableCount == null || sourceTableCount == 0) {
                    logger.warn("表 ddlTestSource 不存在，可能需要重新创建");
                }
                
                // 检查目标表是否存在
                String checkTargetTableSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ddlTestTarget'";
                Integer targetTableCount = databaseTemplate.queryForObject(checkTargetTableSql, Integer.class);
                if (targetTableCount == null || targetTableCount == 0) {
                    logger.warn("表 ddlTestTarget 不存在，可能需要重新创建");
                }
                
                // 检查源表的字段列表（应该只有初始字段：id, first_name, last_name, department, created_at）
                String checkSourceColumnsSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ddlTestSource' ORDER BY ORDINAL_POSITION";
                List<String> sourceColumns = databaseTemplate.queryForList(checkSourceColumnsSql, String.class);
                
                if (sourceColumns != null && sourceColumns.size() > 5) {
                    logger.warn("表 ddlTestSource 包含额外字段，可能未正确重置。当前字段: {}", sourceColumns);
                } else {
                    logger.debug("源表结构验证通过，字段数: {}", sourceColumns != null ? sourceColumns.size() : 0);
                }
                
                // 检查目标表的字段列表
                String checkTargetColumnsSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ddlTestTarget' ORDER BY ORDINAL_POSITION";
                List<String> targetColumns = databaseTemplate.queryForList(checkTargetColumnsSql, String.class);
                
                if (targetColumns != null && targetColumns.size() > 5) {
                    logger.warn("表 ddlTestTarget 包含额外字段，可能未正确重置。当前字段: {}", targetColumns);
                } else {
                    logger.debug("目标表结构验证通过，字段数: {}", targetColumns != null ? targetColumns.size() : 0);
                }
                return null;
            });
        } catch (Exception e) {
            logger.debug("验证表结构时出错（可忽略）: {}", e.getMessage());
        }
    }

    /**
     * 强制重置表（当正常重置失败时使用）
     */
    private void forceResetTable() {
        logger.warn("尝试强制重置表结构");
        try {
            String resetSql = loadSqlScriptByDatabaseType("reset-test-table", sourceConfig);
            if (resetSql != null && !resetSql.trim().isEmpty()) {
                // 直接执行SQL，不通过testDatabaseManager
                DatabaseConnectorInstance sourceInstance = new DatabaseConnectorInstance(sourceConfig);
                DatabaseConnectorInstance targetInstance = new DatabaseConnectorInstance(targetConfig);
                
                sourceInstance.execute(databaseTemplate -> {
                    String[] statements = resetSql.split(";");
                    for (String stmt : statements) {
                        String trimmed = stmt.trim();
                        if (!trimmed.isEmpty()) {
                            try {
                                databaseTemplate.execute(trimmed);
                            } catch (Exception e) {
                                logger.debug("执行重置SQL失败（可能可忽略）: {}", trimmed, e);
                            }
                        }
                    }
                    return null;
                });
                
                targetInstance.execute(databaseTemplate -> {
                    String[] statements = resetSql.split(";");
                    for (String stmt : statements) {
                        String trimmed = stmt.trim();
                        if (!trimmed.isEmpty()) {
                            try {
                                databaseTemplate.execute(trimmed);
                            } catch (Exception e) {
                                logger.debug("执行重置SQL失败（可能可忽略）: {}", trimmed, e);
                            }
                        }
                    }
                    return null;
                });
                
                logger.info("强制重置表结构完成");
            }
        } catch (Exception e) {
            logger.error("强制重置表结构失败", e);
        }
    }

    // ==================== ADD COLUMN 测试场景 ====================

    /**
     * 测试ADD COLUMN - 基础添加字段
     */
    @Test
    public void testAddColumn_Basic() throws Exception {
        logger.info("开始测试ADD COLUMN - 基础添加字段");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN age INT";

        // 启动Mapping
        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        // 执行DDL
        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("age", 10000);

        // 验证字段映射已更新
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        assertNotNull("应找到TableGroup列表", tableGroups);
        assertFalse("TableGroup列表不应为空", tableGroups.isEmpty());
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundAgeMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "age".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "age".equals(fm.getTarget().getName()));

        assertTrue("应找到age字段的映射", foundAgeMapping);

        // 验证目标数据库中字段存在
        verifyFieldExistsInTargetDatabase("age", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN基础测试通过");
    }

    /**
     * 测试ADD COLUMN - 带AFTER子句指定位置
     */
    @Test
    public void testAddColumn_WithAfter() throws Exception {
        logger.info("开始测试ADD COLUMN - 带AFTER子句");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN email VARCHAR(100) AFTER first_name";

        // 启动Mapping，并捕获可能的异常
        try {
            logger.info("准备启动Mapping: mappingId={}, metaId={}", mappingId, metaId);
            String result = mappingService.start(mappingId);
            logger.info("Mapping启动调用完成: result={}", result);
        } catch (Exception e) {
            logger.error("Mapping启动失败: mappingId={}, metaId={}, error={}", mappingId, metaId, e.getMessage(), e);
            // 检查Meta状态，看是否有错误信息
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null) {
                logger.error("Meta状态: state={}, errorMessage={}", meta.getState(), meta.getErrorMessage());
            }
            throw e;
        }
        
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("email", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundEmailMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "email".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "email".equals(fm.getTarget().getName()));

        assertTrue("应找到email字段的映射", foundEmailMapping);
        verifyFieldExistsInTargetDatabase("email", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带AFTER子句测试通过");
    }

    /**
     * 测试ADD COLUMN - 带FIRST子句指定位置
     */
    @Test
    public void testAddColumn_WithFirst() throws Exception {
        logger.info("开始测试ADD COLUMN - 带FIRST子句");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN priority INT FIRST";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("priority", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPriorityMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "priority".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "priority".equals(fm.getTarget().getName()));

        assertTrue("应找到priority字段的映射", foundPriorityMapping);
        verifyFieldExistsInTargetDatabase("priority", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带FIRST子句测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值
     */
    @Test
    public void testAddColumn_WithDefault() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN status VARCHAR(20) DEFAULT 'active'";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("status", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundStatusMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "status".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "status".equals(fm.getTarget().getName()));

        assertTrue("应找到status字段的映射", foundStatusMapping);
        verifyFieldExistsInTargetDatabase("status", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带默认值测试通过");
    }

    /**
     * 测试ADD COLUMN - 带NOT NULL约束
     */
    @Test
    public void testAddColumn_WithNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN phone VARCHAR(20) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("phone", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundPhoneMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "phone".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "phone".equals(fm.getTarget().getName()));

        assertTrue("应找到phone字段的映射", foundPhoneMapping);
        verifyFieldExistsInTargetDatabase("phone", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带NOT NULL约束测试通过");
    }

    /**
     * 测试ADD COLUMN - 带默认值和NOT NULL约束
     */
    @Test
    public void testAddColumn_WithDefaultAndNotNull() throws Exception {
        logger.info("开始测试ADD COLUMN - 带默认值和NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN created_by VARCHAR(50) NOT NULL DEFAULT 'system'";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("created_by", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCreatedByMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "created_by".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "created_by".equals(fm.getTarget().getName()));

        assertTrue("应找到created_by字段的映射", foundCreatedByMapping);
        verifyFieldExistsInTargetDatabase("created_by", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带默认值和NOT NULL约束测试通过");
    }

    // ==================== DROP COLUMN 测试场景 ====================

    /**
     * 测试DROP COLUMN - 删除字段
     */
    @Test
    public void testDropColumn_Basic() throws Exception {
        logger.info("开始测试DROP COLUMN - 删除字段");

        String sourceDDL = "ALTER TABLE ddlTestSource DROP COLUMN department";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待DDL DROP处理完成（使用轮询方式）
        waitForDDLDropProcessingComplete("department", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundDepartmentMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "department".equals(fm.getSource().getName()));

        assertFalse("不应找到department字段的映射", foundDepartmentMapping);
        verifyFieldNotExistsInTargetDatabase("department", "ddlTestTarget", targetConfig);

        logger.info("DROP COLUMN测试通过");
    }

    // ==================== MODIFY COLUMN 测试场景 ====================

    /**
     * 测试MODIFY COLUMN - 修改字段长度
     */
    @Test
    public void testModifyColumn_ChangeLength() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 修改字段长度");

        String sourceDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN first_name VARCHAR(100)";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // MODIFY操作不需要等待新字段，只需要验证现有字段映射仍然存在
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("MODIFY COLUMN修改长度测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 修改字段类型
     */
    @Test
    public void testModifyColumn_ChangeType() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 修改字段类型");

        // 先添加一个INT字段用于测试类型修改
        String addColumnDDL = "ALTER TABLE ddlTestSource ADD COLUMN count_num INT";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        waitForDDLProcessingComplete("count_num", 10000);

        String sourceDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN count_num BIGINT";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        // MODIFY操作不需要等待新字段，只需要验证现有字段映射仍然存在
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundCountNumMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "count_num".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "count_num".equals(fm.getTarget().getName()));

        assertTrue("应找到count_num字段的映射", foundCountNumMapping);

        logger.info("MODIFY COLUMN修改类型测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 添加NOT NULL约束
     */
    @Test
    public void testModifyColumn_AddNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 添加NOT NULL约束");

        String sourceDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN first_name VARCHAR(50) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // MODIFY操作不需要等待新字段，只需要验证现有字段映射仍然存在
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("MODIFY COLUMN添加NOT NULL约束测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 移除NOT NULL约束（允许NULL）
     */
    @Test
    public void testModifyColumn_RemoveNotNull() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 移除NOT NULL约束");

        // 先确保字段是NOT NULL的
        String setNotNullDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN first_name VARCHAR(50) NOT NULL";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(setNotNullDDL, sourceConfig);
        // MODIFY操作不需要等待新字段，只需要验证现有字段映射仍然存在
        Thread.sleep(2000);

        String sourceDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN first_name VARCHAR(50) NULL";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        // MODIFY操作不需要等待新字段，只需要验证现有字段映射仍然存在
        Thread.sleep(2000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundFirstNameMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "first_name".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "first_name".equals(fm.getTarget().getName()));

        assertTrue("应找到first_name字段的映射", foundFirstNameMapping);

        logger.info("MODIFY COLUMN移除NOT NULL约束测试通过");
    }

    // ==================== CHANGE COLUMN 测试场景 ====================

    /**
     * 测试CHANGE COLUMN - 重命名字段（仅重命名，不修改类型）
     */
    @Test
    public void testChangeColumn_RenameOnly() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名字段");

        String sourceDDL = "ALTER TABLE ddlTestSource CHANGE COLUMN first_name full_name VARCHAR(50)";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待CHANGE COLUMN处理完成（使用轮询方式）
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

        logger.info("CHANGE COLUMN重命名字段测试通过");
    }

    /**
     * 测试CHANGE COLUMN - 重命名并修改类型
     */
    @Test
    public void testChangeColumn_RenameAndModifyType() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名并修改类型");

        // 先添加一个VARCHAR字段用于测试
        String addColumnDDL = "ALTER TABLE ddlTestSource ADD COLUMN description VARCHAR(100)";
        mappingService.start(mappingId);
        Thread.sleep(2000);
        executeDDLToSourceDatabase(addColumnDDL, sourceConfig);
        waitForDDLProcessingComplete("description", 10000);

        String sourceDDL = "ALTER TABLE ddlTestSource CHANGE COLUMN description desc_text TEXT";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待CHANGE COLUMN处理完成（使用轮询方式）
        waitForDDLProcessingComplete("desc_text", 10000);

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

        logger.info("CHANGE COLUMN重命名并修改类型测试通过");
    }

    /**
     * 测试CHANGE COLUMN - 重命名并修改长度和约束
     */
    @Test
    public void testChangeColumn_RenameAndModifyLengthAndConstraint() throws Exception {
        logger.info("开始测试CHANGE COLUMN - 重命名并修改长度和约束");

        String sourceDDL = "ALTER TABLE ddlTestSource CHANGE COLUMN first_name user_name VARCHAR(100) NOT NULL";

        mappingService.start(mappingId);
        Thread.sleep(2000);
        
        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);
        
        // 等待CHANGE COLUMN处理完成（使用轮询方式）
        waitForDDLProcessingComplete("user_name", 10000);

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

        logger.info("CHANGE COLUMN重命名并修改长度和约束测试通过");
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建Connector
     */
    private String createConnector(String name, DatabaseConfig config) throws Exception {
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
        return connectorService.add(params);
    }

    /**
     * 创建Mapping和TableGroup
     */
    private String createMapping() throws Exception {
        // 先创建Mapping（不包含tableGroups）
        Map<String, String> params = new HashMap<>();
        params.put("name", "MySQL到MySQL测试Mapping");
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment"); // 增量同步（使用 "increment" 而不是 "1"）
        params.put("incrementStrategy", "Log"); // 增量策略：日志监听（MySQL 使用 binlog）
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);
        
        // 创建后需要编辑一次以正确设置增量同步配置（因为 checkAddConfigModel 默认可能是全量同步）
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment"); // 使用 "increment" 而不是 "1"
        editParams.put("incrementStrategy", "Log");
        editParams.put("enableDDL", "true");
        editParams.put("enableInsert", "true");
        editParams.put("enableUpdate", "true");
        editParams.put("enableDelete", "true");
        mappingService.edit(editParams);

        // 然后使用tableGroupService.add()创建TableGroup
        Map<String, String> tableGroupParams = new HashMap<>();
        tableGroupParams.put("mappingId", mappingId);
        tableGroupParams.put("sourceTable", "ddlTestSource");
        tableGroupParams.put("targetTable", "ddlTestTarget");

        // 构建字段映射：id|id,first_name|first_name
        List<String> fieldMappingList = new ArrayList<>();
        fieldMappingList.add("id|id");
        fieldMappingList.add("first_name|first_name");
        tableGroupParams.put("fieldMappings", String.join(",", fieldMappingList));

        tableGroupService.add(tableGroupParams);

        return mappingId;
    }

    /**
     * 执行DDL到源数据库
     * 在执行前会检查并处理可能的字段冲突
     */
    private void executeDDLToSourceDatabase(String sql, DatabaseConfig config) throws Exception {
        // 检查DDL语句类型，如果是ADD COLUMN，先检查字段是否已存在
        if (sql != null && sql.toUpperCase().contains("ADD COLUMN")) {
            String columnName = extractColumnNameFromAddColumn(sql);
            if (columnName != null && columnExists(columnName, "ddlTestSource", config)) {
                logger.warn("字段 {} 已存在，先删除该字段以确保测试的干净状态", columnName);
                String dropColumnSql = String.format("ALTER TABLE ddlTestSource DROP COLUMN %s", columnName);
                try {
                    DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
                    instance.execute(databaseTemplate -> {
                        databaseTemplate.execute(dropColumnSql);
                        return null;
                    });
                    logger.info("已删除已存在的字段: {}", columnName);
                } catch (Exception e) {
                    logger.warn("删除已存在的字段失败，继续执行DDL: {}", e.getMessage());
                }
            }
        }
        
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            databaseTemplate.execute(sql);
            return null;
        });
    }

    /**
     * 从ADD COLUMN语句中提取字段名
     */
    private String extractColumnNameFromAddColumn(String sql) {
        try {
            // 匹配模式: ADD COLUMN column_name ...
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "(?i)ADD\\s+COLUMN\\s+([a-zA-Z_][a-zA-Z0-9_]*)", 
                java.util.regex.Pattern.CASE_INSENSITIVE
            );
            java.util.regex.Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                return matcher.group(1);
            }
        } catch (Exception e) {
            logger.debug("提取字段名失败: {}", e.getMessage());
        }
        return null;
    }

    /**
     * 检查字段是否已存在
     */
    private boolean columnExists(String columnName, String tableName, DatabaseConfig config) {
        try {
            DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
            return instance.execute(databaseTemplate -> {
                String checkSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                                 "WHERE TABLE_SCHEMA = DATABASE() " +
                                 "AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                Integer count = databaseTemplate.queryForObject(checkSql, Integer.class, tableName, columnName);
                return count != null && count > 0;
            });
        } catch (Exception e) {
            logger.debug("检查字段是否存在时出错: {}", e.getMessage());
            return false;
        }
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
     * 加载测试配置文件
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = DDLMysqlIntegrationTest.class.getClassLoader().getResourceAsStream("test.properties")) {
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
     */
    private static String loadSqlScript(String resourcePath) {
        try {
            InputStream input = DDLMysqlIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath);
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
     * 等待Meta进入运行状态
     * 
     * @param metaId Meta ID
     * @param timeoutMs 超时时间（毫秒）
     * @throws InterruptedException 如果等待过程中被中断
     */
    private void waitForMetaRunning(String metaId, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long checkInterval = 200; // 每200ms检查一次
        
        logger.info("等待Meta进入运行状态: metaId={}", metaId);
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null) {
                // 使用info级别记录状态，便于调试
                logger.info("Meta状态检查: metaId={}, state={}, isRunning={}, errorMessage={}", 
                        metaId, meta.getState(), meta.isRunning(), 
                        meta.getErrorMessage() != null && !meta.getErrorMessage().isEmpty() ? meta.getErrorMessage() : "无");
                
                if (meta.isRunning()) {
                    logger.info("Meta {} 已处于运行状态", metaId);
                    // 额外等待一小段时间，确保 Listener 完全启动并开始监听
                    Thread.sleep(1000);
                    return;
                }
                
                // 如果处于错误状态，记录详细信息并立即抛出异常
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
        
        // 超时后再次检查一次，如果仍未运行则抛出异常
        Meta meta = profileComponent.getMeta(metaId);
        assertNotNull("Meta不应为null", meta);
        logger.error("Meta状态检查失败: metaId={}, state={}, isRunning={}, errorMessage={}", 
                metaId, meta.getState(), meta.isRunning(), meta.getErrorMessage());
        assertTrue("Meta应在" + timeoutMs + "ms内进入运行状态，当前状态: " + meta.getState() + 
                   (meta.getErrorMessage() != null ? ", 错误信息: " + meta.getErrorMessage() : ""), 
                   meta.isRunning());
    }
}

