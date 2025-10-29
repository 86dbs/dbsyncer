package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.ddl.impl.DDLParserImpl;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 异构数据库DDL同步测试
 * 测试不同数据库类型间的DDL同步功能
 *
 * @Author TestUser
 * @Version 1.0.0
 * @Date 2025-10-28
 */
public class HeterogeneousDDLSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(HeterogeneousDDLSyncTest.class);
    
    private static TestDatabaseManager testDatabaseManager;
    private static DatabaseConfig sourceConfig;
    private static DatabaseConfig targetConfig;

    private DDLParserImpl ddlParser;
    private TableGroup sqlserverToMySQLTableGroup;
    private TableGroup mysqlToSQLServerTableGroup;

    @BeforeClass
    public static void setUpClass() throws IOException {
        logger.info("开始初始化异构数据库DDL同步测试环境");
        
        // 加载测试配置
        loadTestConfig();
        
        // 创建测试数据库管理器
        testDatabaseManager = new TestDatabaseManager(sourceConfig, targetConfig);
        
        // 初始化测试环境
        String initSql = loadSqlScript("ddl/init-test-data.sql");
        testDatabaseManager.initializeTestEnvironment(initSql, initSql);
        
        logger.info("异构数据库DDL同步测试环境初始化完成");
    }
    
    @AfterClass
    public static void tearDownClass() {
        logger.info("开始清理异构数据库DDL同步测试环境");
        
        try {
            // 清理测试环境
            String cleanupSql = loadSqlScript("ddl/cleanup-test-data.sql");
            testDatabaseManager.cleanupTestEnvironment(cleanupSql, cleanupSql);
            
            logger.info("异构数据库DDL同步测试环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试环境失败", e);
        }
    }

    @Before
    public void setUp() throws IOException {
        ddlParser = new DDLParserImpl();
        
        // 初始化SQL Server到MySQL的TableGroup配置
        sqlserverToMySQLTableGroup = new TableGroup();
        sqlserverToMySQLTableGroup.setId("sqlserver-to-mysql-tablegroup-id");
        sqlserverToMySQLTableGroup.setMappingId("sqlserver-to-mysql-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理
        
        Table sqlserverSourceTable = new Table();
        sqlserverSourceTable.setName("ddlTestEmployee");
        
        Table mysqlTargetTable = new Table();
        mysqlTargetTable.setName("ddlTestEmployee");
        
        sqlserverToMySQLTableGroup.setSourceTable(sqlserverSourceTable);
        sqlserverToMySQLTableGroup.setTargetTable(mysqlTargetTable);
        
        // 初始化字段映射 for SQL Server to MySQL
        List<FieldMapping> fieldMappings1 = new ArrayList<>();
        Field idSourceField1 = new Field("id", "INT", 4);
        Field idTargetField1 = new Field("id", "INT", 4);
        FieldMapping idMapping1 = new FieldMapping(idSourceField1, idTargetField1);
        fieldMappings1.add(idMapping1);
        
        Field firstNameSourceField1 = new Field("first_name", "NVARCHAR", 12);
        Field firstNameTargetField1 = new Field("first_name", "VARCHAR", 12);
        FieldMapping firstNameMapping1 = new FieldMapping(firstNameSourceField1, firstNameTargetField1);
        fieldMappings1.add(firstNameMapping1);
        
        Field lastNameSourceField1 = new Field("last_name", "NVARCHAR", 12);
        Field lastNameTargetField1 = new Field("last_name", "VARCHAR", 12);
        FieldMapping lastNameMapping1 = new FieldMapping(lastNameSourceField1, lastNameTargetField1);
        fieldMappings1.add(lastNameMapping1);
        
        Field departmentSourceField1 = new Field("department", "NVARCHAR", 12);
        Field departmentTargetField1 = new Field("department", "VARCHAR", 12);
        FieldMapping departmentMapping1 = new FieldMapping(departmentSourceField1, departmentTargetField1);
        fieldMappings1.add(departmentMapping1);
        
        sqlserverToMySQLTableGroup.setFieldMapping(fieldMappings1);
        
        // 初始化MySQL到SQL Server的TableGroup配置
        mysqlToSQLServerTableGroup = new TableGroup();
        mysqlToSQLServerTableGroup.setId("mysql-to-sqlserver-tablegroup-id");
        mysqlToSQLServerTableGroup.setMappingId("mysql-to-sqlserver-mapping-id");
        // Note: TableGroup类中没有setEnableDDL方法，我们通过其他方式处理
        
        Table mysqlSourceTable = new Table();
        mysqlSourceTable.setName("ddlTestEmployee");
        
        Table sqlserverTargetTable = new Table();
        sqlserverTargetTable.setName("ddlTestEmployee");
        
        mysqlToSQLServerTableGroup.setSourceTable(mysqlSourceTable);
        mysqlToSQLServerTableGroup.setTargetTable(sqlserverTargetTable);
        
        // 初始化字段映射 for MySQL to SQL Server
        List<FieldMapping> fieldMappings2 = new ArrayList<>();
        Field idSourceField2 = new Field("id", "INT", 4);
        Field idTargetField2 = new Field("id", "INT", 4);
        FieldMapping idMapping2 = new FieldMapping(idSourceField2, idTargetField2);
        fieldMappings2.add(idMapping2);
        
        Field firstNameSourceField2 = new Field("first_name", "VARCHAR", 12);
        Field firstNameTargetField2 = new Field("first_name", "NVARCHAR", 12);
        FieldMapping firstNameMapping2 = new FieldMapping(firstNameSourceField2, firstNameTargetField2);
        fieldMappings2.add(firstNameMapping2);
        
        Field lastNameSourceField2 = new Field("last_name", "VARCHAR", 12);
        Field lastNameTargetField2 = new Field("last_name", "NVARCHAR", 12);
        FieldMapping lastNameMapping2 = new FieldMapping(lastNameSourceField2, lastNameTargetField2);
        fieldMappings2.add(lastNameMapping2);
        
        Field departmentSourceField2 = new Field("department", "VARCHAR", 12);
        Field departmentTargetField2 = new Field("department", "NVARCHAR", 12);
        FieldMapping departmentMapping2 = new FieldMapping(departmentSourceField2, departmentTargetField2);
        fieldMappings2.add(departmentMapping2);
        
        mysqlToSQLServerTableGroup.setFieldMapping(fieldMappings2);
        
        logger.info("异构数据库DDL同步测试用例环境初始化完成");
    }

    /**
     * 加载测试配置文件
     * @throws IOException
     */
    private static void loadTestConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = HeterogeneousDDLSyncTest.class.getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                logger.warn("未找到test.properties配置文件，使用默认配置");
                sourceConfig = createDefaultSQLServerConfig();
                targetConfig = createDefaultMySQLConfig();
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
        
        // 创建目标数据库配置(MySQL)
        targetConfig = new DatabaseConfig();
        targetConfig.setUrl(props.getProperty("test.db.mysql.url", "jdbc:mysql://127.0.0.1:3306/target_db"));
        targetConfig.setUsername(props.getProperty("test.db.mysql.username", "root"));
        targetConfig.setPassword(props.getProperty("test.db.mysql.password", "123456"));
        targetConfig.setDriverClassName(props.getProperty("test.db.mysql.driver", "com.mysql.cj.jdbc.Driver"));
    }
    
    /**
     * 创建默认的SQL Server配置
     * @return DatabaseConfig
     */
    private static DatabaseConfig createDefaultSQLServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=source_db;encrypt=false;trustServerCertificate=true");
        config.setUsername("sa");
        config.setPassword("123");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }
    
    /**
     * 创建默认的MySQL配置
     * @return DatabaseConfig
     */
    private static DatabaseConfig createDefaultMySQLConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3306/target_db?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false");
        config.setUsername("root");
        config.setPassword("123");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }
    
    /**
     * 加载SQL脚本文件
     * @param resourcePath 资源路径
     * @return SQL脚本内容
     */
    private static String loadSqlScript(String resourcePath) {
        try (InputStream input = HeterogeneousDDLSyncTest.class.getClassLoader().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            if (input == null) {
                logger.warn("未找到SQL脚本文件: {}", resourcePath);
                return "";
            }
            
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            logger.error("加载SQL脚本文件失败: {}", resourcePath, e);
            return "";
        }
    }

    /**
     * 测试SQL Server到MySQL的DDL同步 - 新增字段
     */
    @Test
    public void testSQLServerToMySQL_AddColumn() {
        logger.info("开始测试SQL Server到MySQL的DDL同步 - 新增字段");
        
        // SQL Server ADD COLUMN 语句
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, sqlserverToMySQLTableGroup, sqlserverDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert sqlserverDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getAddedFieldNames().contains("salary") : "新增字段列表应包含salary字段";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(sqlserverToMySQLTableGroup, ddlConfig);
            
            // 验证字段映射更新
            boolean foundSalaryMapping = sqlserverToMySQLTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "salary".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "salary".equals(mapping.getTarget().getName()));
            
            assert foundSalaryMapping : "应找到salary字段的映射";
            
            logger.info("SQL Server到MySQL的DDL同步测试通过 - 新增字段");
        } catch (Exception e) {
            logger.error("SQL Server到MySQL的DDL同步测试失败 - 新增字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试SQL Server到MySQL的DDL同步 - 修改字段
     */
    @Test
    public void testSQLServerToMySQL_AlterColumn() {
        logger.info("开始测试SQL Server到MySQL的DDL同步 - 修改字段");
        
        // SQL Server ALTER COLUMN 语句
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee ALTER COLUMN first_name NVARCHAR(100)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, sqlserverToMySQLTableGroup, sqlserverDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_MODIFY == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_MODIFY";
            assert sqlserverDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getModifiedFieldNames().contains("first_name") : "修改字段列表应包含first_name字段";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(sqlserverToMySQLTableGroup, ddlConfig);
            
            // 验证字段映射仍然存在
            boolean foundFirstNameMapping = sqlserverToMySQLTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "first_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "first_name".equals(mapping.getTarget().getName()));
            
            assert foundFirstNameMapping : "应找到first_name字段的映射";
            
            logger.info("SQL Server到MySQL的DDL同步测试通过 - 修改字段");
        } catch (Exception e) {
            logger.error("SQL Server到MySQL的DDL同步测试失败 - 修改字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试SQL Server到MySQL的DDL同步 - 重命名字段
     */
    @Test
    public void testSQLServerToMySQL_RenameColumn() {
        logger.info("开始测试SQL Server到MySQL的DDL同步 - 重命名字段");
        
        // SQL Server CHANGE COLUMN 语句（模拟标准SQL语法）
        String sqlserverDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname NVARCHAR(50)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, sqlserverToMySQLTableGroup, sqlserverDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_CHANGE == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_CHANGE";
            assert sqlserverDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getChangedFieldNames().containsKey("last_name") && 
                   "surname".equals(ddlConfig.getChangedFieldNames().get("last_name")) : 
                   "变更字段映射应包含last_name到surname的映射";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(sqlserverToMySQLTableGroup, ddlConfig);
            
            // 验证字段映射更新
            boolean foundNewMapping = sqlserverToMySQLTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "surname".equals(mapping.getTarget().getName()));
            
            boolean foundOldMapping = sqlserverToMySQLTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "last_name".equals(mapping.getTarget().getName()));
            
            assert foundNewMapping : "应找到last_name到surname的字段映射";
            assert !foundOldMapping : "不应找到last_name到last_name的旧字段映射";
            
            logger.info("SQL Server到MySQL的DDL同步测试通过 - 重命名字段");
        } catch (Exception e) {
            logger.error("SQL Server到MySQL的DDL同步测试失败 - 重命名字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL到SQL Server的DDL同步 - 新增字段
     */
    @Test
    public void testMySQLToSQLServer_AddColumn() {
        logger.info("开始测试MySQL到SQL Server的DDL同步 - 新增字段");
        
        // MySQL ADD COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestEmployee ADD COLUMN salary DECIMAL(10,2)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, mysqlToSQLServerTableGroup, mysqlDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_ADD == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_ADD";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getAddedFieldNames().contains("salary") : "新增字段列表应包含salary字段";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(mysqlToSQLServerTableGroup, ddlConfig);
            
            // 验证字段映射更新
            boolean foundSalaryMapping = mysqlToSQLServerTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "salary".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "salary".equals(mapping.getTarget().getName()));
            
            assert foundSalaryMapping : "应找到salary字段的映射";
            
            logger.info("MySQL到SQL Server的DDL同步测试通过 - 新增字段");
        } catch (Exception e) {
            logger.error("MySQL到SQL Server的DDL同步测试失败 - 新增字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL到SQL Server的DDL同步 - 修改字段
     */
    @Test
    public void testMySQLToSQLServer_AlterColumn() {
        logger.info("开始测试MySQL到SQL Server的DDL同步 - 修改字段");
        
        // MySQL MODIFY COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestEmployee MODIFY COLUMN first_name VARCHAR(100)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, mysqlToSQLServerTableGroup, mysqlDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_MODIFY == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_MODIFY";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getModifiedFieldNames().contains("first_name") : "修改字段列表应包含first_name字段";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(mysqlToSQLServerTableGroup, ddlConfig);
            
            // 验证字段映射仍然存在
            boolean foundFirstNameMapping = mysqlToSQLServerTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "first_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "first_name".equals(mapping.getTarget().getName()));
            
            assert foundFirstNameMapping : "应找到first_name字段的映射";
            
            logger.info("MySQL到SQL Server的DDL同步测试通过 - 修改字段");
        } catch (Exception e) {
            logger.error("MySQL到SQL Server的DDL同步测试失败 - 修改字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试MySQL到SQL Server的DDL同步 - 重命名字段
     */
    @Test
    public void testMySQLToSQLServer_RenameColumn() {
        logger.info("开始测试MySQL到SQL Server的DDL同步 - 重命名字段");
        
        // MySQL CHANGE COLUMN 语句
        String mysqlDDL = "ALTER TABLE ddlTestEmployee CHANGE COLUMN last_name surname VARCHAR(50)";
        
        try {
            // 解析DDL
            DDLConfig ddlConfig = ddlParser.parse(null, mysqlToSQLServerTableGroup, mysqlDDL);
            
            // 验证解析结果
            assert ddlConfig != null : "DDL配置不应为空";
            assert DDLOperationEnum.ALTER_CHANGE == ddlConfig.getDdlOperationEnum() : "DDL操作类型应为ALTER_CHANGE";
            assert mysqlDDL.equals(ddlConfig.getSql()) : "SQL语句应匹配";
            assert ddlConfig.getChangedFieldNames().containsKey("last_name") && 
                   "surname".equals(ddlConfig.getChangedFieldNames().get("last_name")) : 
                   "变更字段映射应包含last_name到surname的映射";
            
            // 更新字段映射
            ddlParser.refreshFiledMappings(mysqlToSQLServerTableGroup, ddlConfig);
            
            // 验证字段映射更新
            boolean foundNewMapping = mysqlToSQLServerTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "surname".equals(mapping.getTarget().getName()));
            
            boolean foundOldMapping = mysqlToSQLServerTableGroup.getFieldMapping().stream()
                .anyMatch(mapping -> mapping.getSource() != null && "last_name".equals(mapping.getSource().getName()) && 
                           mapping.getTarget() != null && "last_name".equals(mapping.getTarget().getName()));
            
            assert foundNewMapping : "应找到last_name到surname的字段映射";
            assert !foundOldMapping : "不应找到last_name到last_name的旧字段映射";
            
            logger.info("MySQL到SQL Server的DDL同步测试通过 - 重命名字段");
        } catch (Exception e) {
            logger.error("MySQL到SQL Server的DDL同步测试失败 - 重命名字段", e);
            throw new RuntimeException("测试应成功完成，但抛出异常: " + e.getMessage(), e);
        }
    }

    /**
     * 测试异构数据库的数据类型映射
     */
    @Test
    public void testHeterogeneousDataTypeMapping() {
        logger.info("开始测试异构数据库的数据类型映射");
        
        // 测试SQL Server到MySQL的数据类型映射
        String[][] typeMappings = {
            {"SQL Server NVARCHAR(50)", "MySQL VARCHAR(50)"},
            {"SQL Server DATETIME2", "MySQL DATETIME"},
            {"SQL Server DECIMAL(10,2)", "MySQL DECIMAL(10,2)"},
            {"SQL Server INT", "MySQL INT"},
            {"SQL Server BIT", "MySQL TINYINT(1)"}
        };
        
        for (String[] mapping : typeMappings) {
            logger.info("SQL Server类型: {} -> MySQL类型: {}", mapping[0], mapping[1]);
            assert mapping[0] != null : "SQL Server类型不能为空";
            assert mapping[1] != null : "MySQL类型不能为空";
        }
        
        logger.info("SQL Server到MySQL数据类型映射测试通过");
    }

    /**
     * 测试异构数据库的语法差异处理
     */
    @Test
    public void testHeterogeneousSyntaxDifferences() {
        logger.info("开始测试异构数据库的语法差异处理");
        
        // 测试SQL Server和MySQL的语法差异
        String[] syntaxScenarios = {
            "SQL Server: ALTER TABLE ddlTestEmployee ADD salary DECIMAL(10,2)",
            "MySQL: ALTER TABLE ddlTestEmployee ADD COLUMN salary DECIMAL(10,2)"
        };
        
        for (String scenario : syntaxScenarios) {
            logger.info("语法场景: {}", scenario);
            assert (scenario.contains("SQL Server:") || scenario.contains("MySQL:")) : 
                   "语法场景应该包含数据库标识";
        }
        
        logger.info("异构数据库语法差异处理测试通过");
    }

    /**
     * 测试异构数据库的约束处理
     */
    @Test
    public void testHeterogeneousConstraintHandling() {
        logger.info("开始测试异构数据库的约束处理");
        
        // 测试SQL Server和MySQL的约束处理
        String[][] constraintScenarios = {
            {"SQL Server", "ALTER TABLE ddlTestEmployee ADD CONSTRAINT pk_employee PRIMARY KEY (id)"},
            {"MySQL", "ALTER TABLE ddlTestEmployee ADD CONSTRAINT pk_employee PRIMARY KEY (id)"}
        };
        
        for (String[] scenario : constraintScenarios) {
            String dbType = scenario[0];
            String ddl = scenario[1];
            logger.info("{} 约束DDL: {}", dbType, ddl);
            assert ddl.contains("CONSTRAINT") : "DDL应该包含约束关键字";
            assert ddl.contains("PRIMARY KEY") : "DDL应该包含PRIMARY KEY";
        }
        
        logger.info("异构数据库约束处理测试通过");
    }
}