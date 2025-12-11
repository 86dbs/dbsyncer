package org.dbsyncer.web.integration;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.schema.SchemaResolver;
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
public class DDLMysqlIntegrationTest extends BaseDDLIntegrationTest {

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化DDL同步集成测试环境");

        // 加载测试配置
        loadTestConfigStatic();

        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);

        // 初始化测试环境（使用按数据库类型分类的脚本）
        String initSql = loadSqlScriptByDatabaseTypeStatic("reset-test-table", "mysql", DDLMysqlIntegrationTest.class);
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);

        logger.info("DDL同步集成测试环境初始化完成");
    }

    /**
     * 静态方法版本的loadTestConfig，用于@BeforeClass
     */
    private static void loadTestConfigStatic() throws IOException {
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

    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理DDL同步集成测试环境");

        try {
            // 清理测试环境（使用按数据库类型分类的脚本）
            String cleanupSql = loadSqlScriptByDatabaseTypeStatic("cleanup-test-data", "mysql", DDLMysqlIntegrationTest.class);
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);

            logger.info("DDL同步集成测试环境清理完成");
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

        // 创建Connector
        sourceConnectorId = createConnector(getSourceConnectorName(), sourceConfig, true);
        targetConnectorId = createConnector(getTargetConnectorName(), targetConfig, false);

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

        // 验证字段位置：email应该在first_name之后
        verifyFieldPosition("email", "first_name", "ddlTestTarget", targetConfig, true);

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

        // 验证字段位置：priority应该在第一位
        verifyFieldIsFirst("priority", "ddlTestTarget", targetConfig);

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

        // 验证字段默认值：status应该有默认值'active'
        verifyFieldDefaultValue("status", "ddlTestTarget", targetConfig, "'active'");

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

        // 验证字段NOT NULL约束
        verifyFieldNotNull("phone", "ddlTestTarget", targetConfig);

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

        // 验证字段默认值和NOT NULL约束
        verifyFieldDefaultValue("created_by", "ddlTestTarget", targetConfig, "'system'");
        verifyFieldNotNull("created_by", "ddlTestTarget", targetConfig);

        logger.info("ADD COLUMN带默认值和NOT NULL约束测试通过");
    }

    /**
     * 测试ADD COLUMN - 带COMMENT（包含特殊字符）
     * 验证COMMENT字符串中包含单引号、反斜杠、分号、冒号等特殊字符时的转义处理
     */
    @Test
    public void testAddColumn_WithCommentContainingSpecialChars() throws Exception {
        logger.info("开始测试ADD COLUMN - 带COMMENT（包含特殊字符）");

        // 测试COMMENT中包含单引号、分号、冒号等特殊字符
        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN outQrcodeID INT NOT NULL COMMENT '外部活码类型，1：进群宝；2：企业微信'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("outQrcodeID", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundOutQrcodeIDMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "outQrcodeID".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "outQrcodeID".equals(fm.getTarget().getName()));

        assertTrue("应找到outQrcodeID字段的映射", foundOutQrcodeIDMapping);
        verifyFieldExistsInTargetDatabase("outQrcodeID", "ddlTestTarget", targetConfig);

        // 验证字段COMMENT是否正确（包含特殊字符）
        verifyFieldComment("outQrcodeID", "ddlTestTarget", targetConfig, "外部活码类型，1：进群宝；2：企业微信");

        logger.info("ADD COLUMN带COMMENT（包含特殊字符）测试通过");
    }

    /**
     * 测试ADD COLUMN - 带COMMENT（包含单引号和反斜杠）
     * 验证COMMENT字符串中包含单引号和反斜杠时的转义处理
     */
    @Test
    public void testAddColumn_WithCommentContainingQuotesAndBackslash() throws Exception {
        logger.info("开始测试ADD COLUMN - 带COMMENT（包含单引号和反斜杠）");

        // 测试COMMENT中包含单引号和反斜杠
        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN membertype INT NOT NULL COMMENT ' 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("membertype", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundMembertypeMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "membertype".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "membertype".equals(fm.getTarget().getName()));

        assertTrue("应找到membertype字段的映射", foundMembertypeMapping);
        verifyFieldExistsInTargetDatabase("membertype", "ddlTestTarget", targetConfig);

        // 验证字段COMMENT是否正确（包含单引号、分号、冒号等特殊字符）
        String expectedComment = " 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户";
        verifyFieldComment("membertype", "ddlTestTarget", targetConfig, expectedComment);

        logger.info("ADD COLUMN带COMMENT（包含单引号和反斜杠）测试通过");
    }

    /**
     * 测试ADD COLUMN - 带COMMENT（包含分号和冒号）
     * 验证COMMENT字符串中包含分号和冒号时的转义处理
     */
    @Test
    public void testAddColumn_WithCommentContainingSemicolonAndColon() throws Exception {
        logger.info("开始测试ADD COLUMN - 带COMMENT（包含分号和冒号）");

        // 测试COMMENT中包含分号和冒号
        String sourceDDL = "ALTER TABLE ddlTestSource ADD COLUMN typeState INT NOT NULL COMMENT '1:图片；2网页; 3文件；4视频；5小程序'";

        mappingService.start(mappingId);
        Thread.sleep(2000);

        // 验证meta状态为running后再执行DDL，确保 Listener 已完全启动
        waitForMetaRunning(metaId, 10000);

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 等待DDL处理完成（使用轮询方式）
        waitForDDLProcessingComplete("typeState", 10000);

        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        TableGroup tableGroup = tableGroups.get(0);

        boolean foundTypeStateMapping = tableGroup.getFieldMapping().stream()
                .anyMatch(fm -> fm.getSource() != null && "typeState".equals(fm.getSource().getName()) &&
                        fm.getTarget() != null && "typeState".equals(fm.getTarget().getName()));

        assertTrue("应找到typeState字段的映射", foundTypeStateMapping);
        verifyFieldExistsInTargetDatabase("typeState", "ddlTestTarget", targetConfig);

        // 验证字段COMMENT是否正确（包含分号和冒号）
        verifyFieldComment("typeState", "ddlTestTarget", targetConfig, "1:图片；2网页; 3文件；4视频；5小程序");

        logger.info("ADD COLUMN带COMMENT（包含分号和冒号）测试通过");
    }

    /**
     * 测试MODIFY COLUMN - 修改字段并添加COMMENT（包含特殊字符）
     * 验证MODIFY COLUMN时COMMENT中包含特殊字符的转义处理
     */
    @Test
    public void testModifyColumn_WithCommentContainingSpecialChars() throws Exception {
        logger.info("开始测试MODIFY COLUMN - 添加COMMENT（包含特殊字符）");

        // 测试COMMENT中包含单引号（需要转义为''）
        String sourceDDL = "ALTER TABLE ddlTestSource MODIFY COLUMN first_name VARCHAR(50) COMMENT '用户姓名，包含单引号''测试'";

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

        // 验证字段COMMENT是否正确（包含单引号，在MySQL中单引号会被转义为''）
        verifyFieldComment("first_name", "ddlTestTarget", targetConfig, "用户姓名，包含单引号'测试");

        logger.info("MODIFY COLUMN添加COMMENT（包含特殊字符）测试通过");
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

        // 验证字段长度已修改为100
        verifyFieldLength("first_name", "ddlTestTarget", targetConfig, 100);

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

        // 验证字段类型已修改为BIGINT
        verifyFieldType("count_num", "ddlTestTarget", targetConfig, "bigint");

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

        // 验证字段NOT NULL约束
        verifyFieldNotNull("first_name", "ddlTestTarget", targetConfig);

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

        // 验证字段可空（NULL约束）
        verifyFieldNullable("first_name", "ddlTestTarget", targetConfig);

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

        // 验证字段类型已修改为TEXT
        verifyFieldType("desc_text", "ddlTestTarget", targetConfig, "text");

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

        // 验证字段长度已修改为100，并且有NOT NULL约束
        verifyFieldLength("user_name", "ddlTestTarget", targetConfig, 100);
        verifyFieldNotNull("user_name", "ddlTestTarget", targetConfig);

        logger.info("CHANGE COLUMN重命名并修改长度和约束测试通过");
    }

    // ==================== CREATE TABLE 测试场景 ====================

    /**
     * 测试CREATE TABLE - 基础建表（多个字段、主键）
     * 测试配置阶段的建表功能，验证COMMENT转义
     */
    @Test
    public void testCreateTable_Basic() throws Exception {
        logger.info("开始测试CREATE TABLE - 基础建表（配置阶段）");

        // 准备：确保表不存在
        prepareForCreateTableTest("createTableTestSource", "createTableTestTarget");

        // 先在源库创建表
        String sourceDDL = "CREATE TABLE createTableTestSource (" +
                "id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID'," +
                "actID INT NOT NULL COMMENT '活动ID'," +
                "pid INT NOT NULL COMMENT '一级来源'," +
                "mediumID INT NOT NULL COMMENT '二级来源'," +
                "createtime DATETIME NOT NULL COMMENT '创建时间'," +
                "PRIMARY KEY (id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("createTableTestSource", "createTableTestTarget");

        // 验证表结构
        verifyTableExists("createTableTestTarget", targetConfig);
        verifyTableFieldCount("createTableTestTarget", targetConfig, 5);
        verifyFieldExistsInTargetDatabase("id", "createTableTestTarget", targetConfig);
        verifyFieldExistsInTargetDatabase("actID", "createTableTestTarget", targetConfig);
        verifyFieldExistsInTargetDatabase("pid", "createTableTestTarget", targetConfig);
        verifyFieldExistsInTargetDatabase("mediumID", "createTableTestTarget", targetConfig);
        verifyFieldExistsInTargetDatabase("createtime", "createTableTestTarget", targetConfig);

        // 验证主键
        verifyTablePrimaryKeys("createTableTestTarget", targetConfig, Arrays.asList("id"));

        // 验证字段属性
        verifyFieldNotNull("id", "createTableTestTarget", targetConfig);
        verifyFieldComment("id", "createTableTestTarget", targetConfig, "主键ID");
        verifyFieldComment("actID", "createTableTestTarget", targetConfig, "活动ID");

        logger.info("CREATE TABLE基础建表测试通过");
    }

    /**
     * 测试CREATE TABLE - 带COMMENT（包含特殊字符）
     * 重点测试COMMENT转义功能，这是修复的核心场景
     */
    @Test
    public void testCreateTable_WithSpecialCharsInComments() throws Exception {
        logger.info("开始测试CREATE TABLE - 带COMMENT（包含特殊字符）");

        // 准备：确保表不存在
        prepareForCreateTableTest("visit_wechatsale_activity_allocationresult", "visit_wechatsale_activity_allocationresult");

        // 先在源库创建表（包含特殊字符的COMMENT）
        String sourceDDL = "CREATE TABLE visit_wechatsale_activity_allocationresult (" +
                "resultID INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键'," +
                "outQrcodeID INT NOT NULL COMMENT '外部活码类型，1：进群宝；2：企业微信'," +
                "membertype INT NOT NULL COMMENT ' 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户'," +
                "typeState INT NOT NULL COMMENT '1:图片；2网页; 3文件；4视频；5小程序'," +
                "PRIMARY KEY (resultID)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        // 这里会触发 COMMENT 转义逻辑
        createTargetTableFromSource("visit_wechatsale_activity_allocationresult", "visit_wechatsale_activity_allocationresult");

        // 验证表结构
        verifyTableExists("visit_wechatsale_activity_allocationresult", targetConfig);
        verifyTableFieldCount("visit_wechatsale_activity_allocationresult", targetConfig, 4);

        // 验证COMMENT（重点测试特殊字符转义）
        verifyFieldComment("outQrcodeID", "visit_wechatsale_activity_allocationresult", targetConfig,
                "外部活码类型，1：进群宝；2：企业微信");
        verifyFieldComment("membertype", "visit_wechatsale_activity_allocationresult", targetConfig,
                " 1：新学员无交费(无任何交费记录); -- 新用户 2：老学员老交费(当前日期无交费课程); -- 老用户 3：新学员新交费(当前日期在辅导期内);4：老学员新交费(历史有过交费，当前日期也在辅导期内) -- 历史用户");
        verifyFieldComment("typeState", "visit_wechatsale_activity_allocationresult", targetConfig,
                "1:图片；2网页; 3文件；4视频；5小程序");

        // 验证主键
        verifyTablePrimaryKeys("visit_wechatsale_activity_allocationresult", targetConfig, Arrays.asList("resultID"));

        logger.info("CREATE TABLE带COMMENT（包含特殊字符）测试通过");
    }

    /**
     * 测试CREATE TABLE - 带COMMENT（包含单引号）
     * 测试单引号转义功能
     */
    @Test
    public void testCreateTable_WithQuotesInComments() throws Exception {
        logger.info("开始测试CREATE TABLE - 带COMMENT（包含单引号）");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableWithQuotes", "testTableWithQuotes");

        // 先在源库创建表（COMMENT中包含单引号）
        String sourceDDL = "CREATE TABLE testTableWithQuotes (" +
                "id INT NOT NULL AUTO_INCREMENT COMMENT '主键ID'," +
                "name VARCHAR(100) NOT NULL COMMENT '用户''名称'," +
                "description VARCHAR(500) COMMENT '描述信息：包含''单引号''测试'," +
                "PRIMARY KEY (id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableWithQuotes", "testTableWithQuotes");

        // 验证表结构
        verifyTableExists("testTableWithQuotes", targetConfig);

        // 验证COMMENT（单引号应被正确转义）
        verifyFieldComment("name", "testTableWithQuotes", targetConfig, "用户'名称");
        verifyFieldComment("description", "testTableWithQuotes", targetConfig, "描述信息：包含'单引号'测试");

        logger.info("CREATE TABLE带COMMENT（包含单引号）测试通过");
    }

    /**
     * 测试CREATE TABLE - 带约束（NOT NULL、AUTO_INCREMENT）
     */
    @Test
    public void testCreateTable_WithConstraints() throws Exception {
        logger.info("开始测试CREATE TABLE - 带约束");

        // 准备：确保表不存在
        prepareForCreateTableTest("testTableWithConstraints", "testTableWithConstraints");

        // 先在源库创建表（包含各种约束）
        String sourceDDL = "CREATE TABLE testTableWithConstraints (" +
                "id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID'," +
                "username VARCHAR(50) NOT NULL COMMENT '用户名'," +
                "email VARCHAR(100) COMMENT '邮箱（可空）'," +
                "age INT NOT NULL COMMENT '年龄'," +
                "status TINYINT(3) NOT NULL DEFAULT 1 COMMENT '状态值，1有效'," +
                "PRIMARY KEY (id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableWithConstraints", "testTableWithConstraints");

        // 验证表结构
        verifyTableExists("testTableWithConstraints", targetConfig);
        verifyTableFieldCount("testTableWithConstraints", targetConfig, 5);

        // 验证约束
        verifyFieldNotNull("id", "testTableWithConstraints", targetConfig);
        verifyFieldNotNull("username", "testTableWithConstraints", targetConfig);
        verifyFieldNotNull("age", "testTableWithConstraints", targetConfig);
        verifyFieldNotNull("status", "testTableWithConstraints", targetConfig);
        verifyFieldNullable("email", "testTableWithConstraints", targetConfig);

        // 验证主键
        verifyTablePrimaryKeys("testTableWithConstraints", targetConfig, Arrays.asList("id"));

        // 验证COMMENT
        verifyFieldComment("id", "testTableWithConstraints", targetConfig, "主键ID");
        verifyFieldComment("username", "testTableWithConstraints", targetConfig, "用户名");
        verifyFieldComment("status", "testTableWithConstraints", targetConfig, "状态值，1有效");

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

        // 先在源库创建表（复合主键）
        String sourceDDL = "CREATE TABLE testTableCompositePK (" +
                "user_id INT NOT NULL COMMENT '用户ID'," +
                "role_id INT NOT NULL COMMENT '角色ID'," +
                "created_at DATETIME NOT NULL COMMENT '创建时间'," +
                "PRIMARY KEY (user_id, role_id)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        executeDDLToSourceDatabase(sourceDDL, sourceConfig);

        // 模拟配置阶段的建表流程：获取源表结构 -> 生成DDL -> 执行DDL
        createTargetTableFromSource("testTableCompositePK", "testTableCompositePK");

        // 验证表结构
        verifyTableExists("testTableCompositePK", targetConfig);
        verifyTableFieldCount("testTableCompositePK", targetConfig, 3);

        // 验证复合主键
        verifyTablePrimaryKeys("testTableCompositePK", targetConfig, Arrays.asList("user_id", "role_id"));

        logger.info("CREATE TABLE复合主键测试通过");
    }

    // ==================== 辅助方法 ====================

    /**
     * 执行DDL到源数据库
     * 在执行前会检查并处理可能的字段冲突
     */
    @Override
    protected void executeDDLToSourceDatabase(String sql, DatabaseConfig config) throws Exception {
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

        super.executeDDLToSourceDatabase(sql, config);
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
     * 验证字段位置：验证指定字段是否在另一个字段之后（或之前）
     *
     * @param fieldName          要验证的字段名
     * @param referenceFieldName 参考字段名
     * @param tableName          表名
     * @param config             数据库配置
     * @param after              true表示验证fieldName在referenceFieldName之后，false表示之前
     */
    private void verifyFieldPosition(String fieldName, String referenceFieldName, String tableName, DatabaseConfig config, boolean after) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的位置顺序
            String sql = "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                    "AND COLUMN_NAME IN (?, ?) " +
                    "ORDER BY ORDINAL_POSITION";

            List<Map<String, Object>> columns = databaseTemplate.queryForList(sql, tableName, fieldName, referenceFieldName);

            if (columns.size() != 2) {
                throw new AssertionError(String.format("未找到字段 %s 或 %s", fieldName, referenceFieldName));
            }

            int fieldPosition = -1;
            int refPosition = -1;
            for (Map<String, Object> col : columns) {
                String colName = (String) col.get("COLUMN_NAME");
                Integer position = ((Number) col.get("ORDINAL_POSITION")).intValue();
                if (fieldName.equalsIgnoreCase(colName)) {
                    fieldPosition = position;
                } else if (referenceFieldName.equalsIgnoreCase(colName)) {
                    refPosition = position;
                }
            }

            if (after) {
                assertTrue(String.format("字段 %s 应在字段 %s 之后，但实际位置: %s=%d, %s=%d",
                                fieldName, referenceFieldName, fieldName, fieldPosition, referenceFieldName, refPosition),
                        fieldPosition > refPosition);
                logger.info("字段位置验证通过: {} (位置{}) 在 {} (位置{}) 之后", fieldName, fieldPosition, referenceFieldName, refPosition);
            } else {
                assertTrue(String.format("字段 %s 应在字段 %s 之前，但实际位置: %s=%d, %s=%d",
                                fieldName, referenceFieldName, fieldName, fieldPosition, referenceFieldName, refPosition),
                        fieldPosition < refPosition);
                logger.info("字段位置验证通过: {} (位置{}) 在 {} (位置{}) 之前", fieldName, fieldPosition, referenceFieldName, refPosition);
            }

            return null;
        });
    }

    /**
     * 验证字段是否在第一位
     *
     * @param fieldName 要验证的字段名
     * @param tableName 表名
     * @param config    数据库配置
     */
    private void verifyFieldIsFirst(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的位置
            String sql = "SELECT ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

            Integer position = databaseTemplate.queryForObject(sql, Integer.class, tableName, fieldName);

            assertNotNull(String.format("未找到字段 %s", fieldName), position);
            assertEquals(String.format("字段 %s 应在第一位，但实际位置是 %d", fieldName, position),
                    Integer.valueOf(1), position);

            logger.info("字段位置验证通过: {} 在第一位", fieldName);
            return null;
        });
    }

    /**
     * 验证字段的默认值
     *
     * @param fieldName       要验证的字段名
     * @param tableName       表名
     * @param config          数据库配置
     * @param expectedDefault 期望的默认值（SQL格式，如 'active' 或 NULL）
     */
    private void verifyFieldDefaultValue(String fieldName, String tableName, DatabaseConfig config, String expectedDefault) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的默认值
            String sql = "SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

            String actualDefault = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);

            if (expectedDefault == null || "NULL".equalsIgnoreCase(expectedDefault)) {
                assertTrue(String.format("字段 %s 的默认值应为 NULL，但实际是 %s", fieldName, actualDefault),
                        actualDefault == null || "NULL".equalsIgnoreCase(actualDefault));
            } else {
                // MySQL的默认值可能包含引号，需要比较时去除引号
                String normalizedExpected = expectedDefault.replace("'", "").replace("\"", "");
                String normalizedActual = actualDefault != null ? actualDefault.replace("'", "").replace("\"", "") : "";

                assertTrue(String.format("字段 %s 的默认值应为 %s，但实际是 %s", fieldName, expectedDefault, actualDefault),
                        normalizedExpected.equalsIgnoreCase(normalizedActual));
            }

            logger.info("字段默认值验证通过: {} 的默认值是 {}", fieldName, actualDefault);
            return null;
        });
    }

    /**
     * 验证字段是否为NOT NULL
     *
     * @param fieldName 要验证的字段名
     * @param tableName 表名
     * @param config    数据库配置
     */
    private void verifyFieldNotNull(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的IS_NULLABLE属性
            String sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

            String isNullable = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);

            assertNotNull(String.format("未找到字段 %s", fieldName), isNullable);
            assertEquals(String.format("字段 %s 应为NOT NULL，但实际是 %s", fieldName, isNullable),
                    "NO", isNullable.toUpperCase());

            logger.info("字段NOT NULL约束验证通过: {} 是NOT NULL", fieldName);
            return null;
        });
    }

    /**
     * 验证字段是否可空（NULL）
     *
     * @param fieldName 要验证的字段名
     * @param tableName 表名
     * @param config    数据库配置
     */
    private void verifyFieldNullable(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的IS_NULLABLE属性
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
     * 验证字段长度
     *
     * @param fieldName      要验证的字段名
     * @param tableName      表名
     * @param config         数据库配置
     * @param expectedLength 期望的长度
     */
    private void verifyFieldLength(String fieldName, String tableName, DatabaseConfig config, int expectedLength) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的CHARACTER_MAXIMUM_LENGTH属性
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
     * 验证字段类型
     *
     * @param fieldName    要验证的字段名
     * @param tableName    表名
     * @param config       数据库配置
     * @param expectedType 期望的类型（不区分大小写，如 "varchar", "int", "bigint", "text"）
     */
    private void verifyFieldType(String fieldName, String tableName, DatabaseConfig config, String expectedType) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的DATA_TYPE属性
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
     * 验证字段的COMMENT
     *
     * @param fieldName       要验证的字段名
     * @param tableName       表名
     * @param config          数据库配置
     * @param expectedComment 期望的COMMENT内容
     */
    private void verifyFieldComment(String fieldName, String tableName, DatabaseConfig config, String expectedComment) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 查询字段的COLUMN_COMMENT属性
            String sql = "SELECT COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

            String actualComment = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);

            assertNotNull(String.format("未找到字段 %s 或字段没有COMMENT", fieldName), actualComment);

            // 比较COMMENT内容（去除首尾空格）
            String normalizedExpected = expectedComment != null ? expectedComment.trim() : "";
            String normalizedActual = actualComment != null ? actualComment.trim() : "";

            assertTrue(String.format("字段 %s 的COMMENT应为 '%s'，但实际是 '%s'", fieldName, expectedComment, actualComment),
                    normalizedExpected.equals(normalizedActual));

            logger.info("字段COMMENT验证通过: {} 的COMMENT是 '{}'", fieldName, actualComment);
            return null;
        });
    }

    /**
     * 准备建表测试环境（确保表不存在）
     */
    private void prepareForCreateTableTest(String sourceTable, String targetTable) throws Exception {
        logger.debug("准备建表测试环境，确保表不存在: sourceTable={}, targetTable={}", sourceTable, targetTable);

        // 删除源表和目标表（如果存在）
        forceDropTable(sourceTable, sourceConfig);
        forceDropTable(targetTable, sourceConfig);
        forceDropTable(sourceTable, targetConfig);
        forceDropTable(targetTable, targetConfig);

        // 等待删除完成
        Thread.sleep(200);

        logger.debug("建表测试环境准备完成");
    }

    /**
     * 模拟配置阶段的建表流程：从源表结构创建目标表
     * 这是配置阶段建表的核心逻辑，会触发 COMMENT 转义功能
     */
    private void createTargetTableFromSource(String sourceTable, String targetTable) throws Exception {
        logger.info("开始从源表创建目标表: {} -> {}", sourceTable, targetTable);

        // 确保 connectorType 已设置，否则 ConnectorFactory 无法找到对应的 ConnectorService
        if (sourceConfig.getConnectorType() == null) {
            sourceConfig.setConnectorType(getConnectorType(sourceConfig, true));
        }
        if (targetConfig.getConnectorType() == null) {
            targetConfig.setConnectorType(getConnectorType(targetConfig, false));
        }

        // 1. 连接源和目标数据库
        ConnectorInstance sourceConnectorInstance = connectorFactory.connect(sourceConfig);
        ConnectorInstance targetConnectorInstance = connectorFactory.connect(targetConfig);

        // 2. 检查目标表是否已存在（避免重复创建）
        try {
            MetaInfo existingTable = connectorFactory.getMetaInfo(targetConnectorInstance, targetTable);
            if (existingTable != null && existingTable.getColumn() != null && !existingTable.getColumn().isEmpty()) {
                logger.info("目标表已存在，跳过创建: {}", targetTable);
                return;
            }
        } catch (Exception e) {
            // 表不存在，继续创建流程
            logger.debug("目标表不存在，开始创建: {}", targetTable);
        }

        // 3. 获取源表结构
        MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(sourceConnectorInstance, sourceTable);
        assertNotNull("无法获取源表结构: " + sourceTable, sourceMetaInfo);
        assertFalse("源表没有字段: " + sourceTable, sourceMetaInfo.getColumn() == null || sourceMetaInfo.getColumn().isEmpty());

        // 4. 判断是否为同类型数据库（都是MySQL）
        String sourceType = sourceConfig.getConnectorType();
        String targetType = targetConfig.getConnectorType();
        boolean isSameType = sourceType != null && sourceType.equals(targetType);

        String createTableDDL;

        if (isSameType) {
            // 同类型数据库优化：直接使用源 MetaInfo，跳过类型转换
            logger.debug("检测到同类型数据库（{}），使用优化路径创建表", sourceType);

            // 获取目标连接器的 SqlTemplate
            // 注意：connectorFactory.getConnectorService() 返回的是 SDK 的 ConnectorService
            org.dbsyncer.sdk.spi.ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetType);
            if (!(targetConnectorService instanceof AbstractDatabaseConnector)) {
                throw new UnsupportedOperationException("目标连接器不支持直接访问 SqlTemplate: " + targetType);
            }

            AbstractDatabaseConnector targetDatabaseConnector =
                    (AbstractDatabaseConnector) targetConnectorService;
            SqlTemplate sqlTemplate = targetDatabaseConnector.sqlTemplate;
            if (sqlTemplate == null) {
                throw new UnsupportedOperationException("目标连接器未初始化 SqlTemplate: " + targetType);
            }

            // 提取字段和主键
            List<Field> fields = sourceMetaInfo.getColumn();
            List<String> primaryKeys = new ArrayList<>();
            for (Field field : fields) {
                if (field.isPk()) {
                    primaryKeys.add(field.getName());
                }
            }

            // 直接调用 buildCreateTableSql，跳过类型转换
            // 这里会触发 COMMENT 转义逻辑（在 MySQLTemplate.escapeMySQLString 中）
            createTableDDL = sqlTemplate.buildCreateTableSql(null, targetTable, fields, primaryKeys);
            logger.debug("使用优化路径生成 CREATE TABLE DDL（跳过类型转换）");
        } else {
            // 不同类型数据库：走标准转换流程
            logger.debug("检测到不同类型数据库（{} -> {}），使用标准转换流程", sourceType, targetType);

            // 注意：connectorFactory.getConnectorService() 返回的是 SDK 的 ConnectorService
            org.dbsyncer.sdk.spi.ConnectorService sourceConnectorService = connectorFactory.getConnectorService(sourceType);
            org.dbsyncer.sdk.spi.ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetType);
            SchemaResolver sourceSchemaResolver = sourceConnectorService.getSchemaResolver();

            // 创建标准化的 MetaInfo
            MetaInfo standardizedMetaInfo = new MetaInfo();
            standardizedMetaInfo.setTableType(sourceMetaInfo.getTableType());
            standardizedMetaInfo.setSql(sourceMetaInfo.getSql());
            standardizedMetaInfo.setIndexType(sourceMetaInfo.getIndexType());

            // 将源字段转换为标准类型（toStandardType 会自动保留所有元数据属性，包括 COMMENT）
            List<Field> standardizedFields = new ArrayList<>();
            for (Field sourceField : sourceMetaInfo.getColumn()) {
                Field standardField = sourceSchemaResolver.toStandardType(sourceField);
                standardizedFields.add(standardField);
            }
            standardizedMetaInfo.setColumn(standardizedFields);

            // 生成 CREATE TABLE DDL（使用标准化后的 MetaInfo）
            // 这里会触发 COMMENT 转义逻辑
            createTableDDL = targetConnectorService.generateCreateTableDDL(standardizedMetaInfo, targetTable);
        }

        // 5. 执行 CREATE TABLE DDL
        assertNotNull("无法生成 CREATE TABLE DDL", createTableDDL);
        assertFalse("生成的 CREATE TABLE DDL 为空", createTableDDL.trim().isEmpty());

        DDLConfig ddlConfig = new DDLConfig();
        ddlConfig.setSql(createTableDDL);
        Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig);

        if (result != null && result.error != null && !result.error.trim().isEmpty()) {
            throw new RuntimeException("创建表失败: " + result.error);
        }

        logger.info("成功创建目标表: {}", targetTable);
    }

    /**
     * 验证表是否存在
     */
    private void verifyTableExists(String tableName, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
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
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
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
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                    "AND CONSTRAINT_NAME LIKE 'PRIMARY' " +
                    "ORDER BY ORDINAL_POSITION";

            List<String> actualKeys = databaseTemplate.queryForList(sql, String.class, tableName);

            assertNotNull(String.format("未找到表 %s 的主键信息", tableName), actualKeys);
            assertEquals(String.format("表 %s 的主键数量应为 %d，但实际是 %d", tableName, expectedKeys.size(), actualKeys.size()),
                    expectedKeys.size(), actualKeys.size());

            // 验证主键列名
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


    // ==================== 抽象方法实现 ====================

    @Override
    protected Class<?> getTestClass() {
        return DDLMysqlIntegrationTest.class;
    }

    @Override
    protected String getSourceConnectorName() {
        return "MySQL源连接器";
    }

    @Override
    protected String getTargetConnectorName() {
        return "MySQL目标连接器";
    }

    @Override
    protected String getMappingName() {
        return "MySQL到MySQL测试Mapping";
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
        return fieldMappingList;
    }

    @Override
    protected String getConnectorType(DatabaseConfig config, boolean isSource) {
        // MySQL 连接器类型
        return "MySQL";
    }

    @Override
    protected String getIncrementStrategy() {
        return "Log"; // MySQL 使用 binlog
    }

    @Override
    protected String getDatabaseType(boolean isSource) {
        return "mysql"; // MySQL 数据库类型
    }
}

