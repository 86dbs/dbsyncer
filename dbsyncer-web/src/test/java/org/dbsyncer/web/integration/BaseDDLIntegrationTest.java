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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * DDL集成测试基类
 * 提供所有DDL集成测试的公共方法和工具
 */
public abstract class BaseDDLIntegrationTest {

    protected static final Logger logger = LoggerFactory.getLogger(BaseDDLIntegrationTest.class);

    @Resource
    protected ConnectorService connectorService;

    @Resource
    protected MappingService mappingService;

    @Resource
    protected ProfileComponent profileComponent;

    @Resource
    protected ConnectorFactory connectorFactory;

    @Resource
    protected TableGroupService tableGroupService;

    protected static DatabaseConfig sourceConfig;
    protected static DatabaseConfig targetConfig;
    protected static TestDatabaseManager testDatabaseManager;

    protected String sourceConnectorId;
    protected String targetConnectorId;
    protected String mappingId;
    protected String metaId;

    // ==================== 抽象方法：子类需要实现 ====================

    /**
     * 获取测试类名称，用于日志和配置加载
     */
    protected abstract Class<?> getTestClass();

    /**
     * 加载测试配置
     * 子类可以覆盖此方法以提供特定的配置加载逻辑
     */
    protected abstract void loadTestConfig() throws IOException;

    /**
     * 创建源连接器名称
     */
    protected abstract String getSourceConnectorName();

    /**
     * 创建目标连接器名称
     */
    protected abstract String getTargetConnectorName();

    /**
     * 创建Mapping名称
     */
    protected abstract String getMappingName();

    /**
     * 获取源表名
     */
    protected abstract String getSourceTableName();

    /**
     * 获取目标表名
     */
    protected abstract String getTargetTableName();

    /**
     * 获取初始字段映射列表
     */
    protected abstract List<String> getInitialFieldMappings();

    /**
     * 获取连接器类型（用于createConnector）
     *
     * @param config   数据库配置
     * @param isSource 是否为源连接器
     * @return 连接器类型
     */
    protected abstract String getConnectorType(DatabaseConfig config, boolean isSource);

    /**
     * 获取增量策略
     */
    protected abstract String getIncrementStrategy();

    /**
     * 获取数据库类型（用于加载对应的SQL脚本）
     *
     * @param isSource 是否为源数据库
     * @return 数据库类型字符串（如 "mysql", "sqlserver"）
     */
    protected abstract String getDatabaseType(boolean isSource);

    // ==================== 公共配置和工具方法 ====================

    /**
     * 根据数据库类型加载对应的SQL脚本
     *
     * @param scriptBaseName 脚本基础名称（不包含数据库类型后缀和扩展名）
     * @param isSource       是否为源数据库
     * @return SQL脚本内容
     */
    protected String loadSqlScriptByDatabaseType(String scriptBaseName, boolean isSource) {
        String dbType = getDatabaseType(isSource);
        String resourcePath = String.format("ddl/%s-%s.sql", scriptBaseName, dbType);
        return loadSqlScript(resourcePath, getTestClass());
    }

    /**
     * 根据数据库类型加载对应的SQL脚本（静态版本，用于@BeforeClass/@AfterClass）
     *
     * @param scriptBaseName 脚本基础名称（不包含数据库类型后缀和扩展名）
     * @param dbType         数据库类型字符串（如 "mysql", "sqlserver"）
     * @param clazz          测试类
     * @return SQL脚本内容
     */
    protected static String loadSqlScriptByDatabaseTypeStatic(String scriptBaseName, String dbType, Class<?> clazz) {
        String resourcePath = String.format("ddl/%s-%s.sql", scriptBaseName, dbType);
        return loadSqlScript(resourcePath, clazz);
    }

    /**
     * 加载SQL脚本文件
     */
    protected static String loadSqlScript(String resourcePath, Class<?> clazz) {
        try {
            InputStream input = clazz.getClassLoader().getResourceAsStream(resourcePath);
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
     * 创建默认的MySQL配置
     */
    protected static DatabaseConfig createDefaultMySQLConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false");
        config.setUsername("root");
        config.setPassword("123");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }

    /**
     * 创建默认的SQL Server配置
     */
    protected static DatabaseConfig createDefaultSQLServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test;encrypt=false;trustServerCertificate=true");
        config.setUsername("sa");
        config.setPassword("123");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }

    // ==================== 公共测试环境管理方法 ====================

    /**
     * 清理残留的测试 mapping
     * 防止上一个测试清理失败导致残留，确保每个测试开始时环境干净
     */
    protected void cleanupResidualTestMappings() {
        try {
            List<Mapping> allMappings = profileComponent.getMappingAll();
            int cleanedCount = 0;

            for (Mapping mapping : allMappings) {
                String mappingName = mapping.getName();
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

            if (cleanedCount > 0) {
                logger.info("清理完成，共清理了 {} 个残留的测试 mapping", cleanedCount);
            }
        } catch (Exception e) {
            logger.debug("清理残留测试 mapping 时出错: {}", e.getMessage());
        }
    }

    /**
     * 重置数据库表结构到初始状态
     * 子类可以覆盖此方法以提供特定的重置逻辑
     */
    protected void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构");
        try {
            String resetSql = loadSqlScriptByDatabaseType("reset-test-table", true);
            if (resetSql != null && !resetSql.trim().isEmpty()) {
                testDatabaseManager.resetTableStructure(resetSql);
                logger.debug("测试数据库表结构重置完成");
            } else {
                logger.warn("重置SQL脚本为空，无法重置表结构");
            }
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
        }
    }

    // ==================== 公共Connector和Mapping创建方法 ====================

    /**
     * 创建Connector
     */
    protected String createConnector(String name, DatabaseConfig config, boolean isSource) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", name);
        params.put("connectorType", getConnectorType(config, isSource));
        params.put("url", config.getUrl());
        params.put("username", config.getUsername());
        params.put("password", config.getPassword());
        params.put("driverClassName", config.getDriverClassName());
        if (config.getSchema() != null) {
            params.put("schema", config.getSchema());
        } else {
            String connectorType = getConnectorType(config, isSource);
            if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                params.put("schema", "dbo"); // SQL Server 默认 schema
            }
        }
        return connectorService.add(params);
    }

    /**
     * 创建Mapping和TableGroup
     */
    protected String createMapping() throws Exception {
        // 先创建Mapping（不包含tableGroups）
        Map<String, String> params = new HashMap<>();
        params.put("name", getMappingName());
        params.put("sourceConnectorId", sourceConnectorId);
        params.put("targetConnectorId", targetConnectorId);
        params.put("model", "increment"); // 增量同步
        params.put("incrementStrategy", getIncrementStrategy());
        params.put("enableDDL", "true");
        params.put("enableInsert", "true");
        params.put("enableUpdate", "true");
        params.put("enableDelete", "true");

        String mappingId = mappingService.add(params);

        // 创建后需要编辑一次以正确设置增量同步配置
        Map<String, String> editParams = new HashMap<>();
        editParams.put("id", mappingId);
        editParams.put("model", "increment");
        editParams.put("incrementStrategy", getIncrementStrategy());
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

    // ==================== 公共DDL执行方法 ====================

    /**
     * 执行DDL到源数据库
     */
    protected void executeDDLToSourceDatabase(String sql, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            databaseTemplate.execute(sql);
            return null;
        });
    }

    // ==================== 公共验证方法 ====================

    /**
     * 验证目标数据库中字段是否存在
     */
    protected void verifyFieldExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
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
    protected void verifyFieldNotExistsInTargetDatabase(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        ConnectorInstance<DatabaseConfig, ?> instance = connectorFactory.connect(config);
        MetaInfo metaInfo = connectorFactory.getMetaInfo(instance, tableName);
        boolean exists = metaInfo.getColumn().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field.getName()));
        assertFalse(String.format("目标数据库表 %s 不应包含字段 %s", tableName, fieldName), exists);
    }

    // ==================== 公共等待方法 ====================

    /**
     * 等待DDL处理完成（通过轮询检查字段映射是否已更新）
     */
    protected void waitForDDLProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
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
    protected void waitForDDLDropProcessingComplete(String expectedFieldName, long timeoutMs) throws InterruptedException, Exception {
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
    protected void waitForMetaRunning(String metaId, long timeoutMs) throws InterruptedException {
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

