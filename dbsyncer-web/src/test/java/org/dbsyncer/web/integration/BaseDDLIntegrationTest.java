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
     * 确保源表和目标表都被正确重置，为下次测试做好准备
     * 子类可以覆盖此方法以提供特定的重置逻辑
     */
    protected void resetDatabaseTableStructure() {
        logger.debug("开始重置测试数据库表结构（源表和目标表）");
        try {
            // 加载源数据库的重置脚本
            String sourceResetSql = loadSqlScriptByDatabaseType("reset-test-table", true);
            // 加载目标数据库的重置脚本（对于同构数据库，通常与源数据库相同）
            String targetResetSql = loadSqlScriptByDatabaseType("reset-test-table", false);
            
            if (sourceResetSql != null && !sourceResetSql.trim().isEmpty() &&
                targetResetSql != null && !targetResetSql.trim().isEmpty()) {
                // 同时重置源表和目标表，确保测试间的隔离性
                testDatabaseManager.resetTableStructure(sourceResetSql, targetResetSql);
                logger.debug("测试数据库表结构重置完成（源表和目标表已重置）");
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
        logger.info("在源库执行了sql: {}", sql);
    }

    /**
     * 执行DML到源数据库（用于触发 Change Tracking 版本号变化，从而触发 DDL 检测）
     * SQL Server CT 模式下，DDL 检测依赖于 DML 操作来触发版本号变化
     */
    protected void executeDMLToSourceDatabase(String tableName, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            // 执行一个简单的 UPDATE 操作来触发 Change Tracking 版本号变化
            // 使用 WHERE 1=0 确保不影响实际数据
            String updateSql = String.format("UPDATE [%s] SET id = id WHERE 1=0", tableName);
            databaseTemplate.execute(updateSql);
            return null;
        });
    }

    /**
     * 执行 INSERT DML 到源数据库并返回插入的数据（用于验证 DDL 和 DML 的数据绑定关系）
     * 先定义数据，然后自动生成 INSERT SQL 并执行
     * 如果数据中不包含 id 字段，则自动生成 id 并返回
     *
     * @param tableName 表名
     * @param data      要插入的数据（Map<字段名, 值>），不应包含 id 字段（id 由数据库自动生成）
     * @param config    数据库配置
     * @return 插入的完整数据（Map<字段名, 值>），包含自动生成的 id，可直接用于验证数据同步
     */
    protected Map<String, Object> executeInsertDMLToSourceDatabase(String tableName, Map<String, Object> data, DatabaseConfig config) throws Exception {
        // 查询表结构，找到IDENTITY/AUTO_INCREMENT列的名称（不区分大小写）
        String identityColumnName = findIdentityColumnName(tableName, config);
        
        // 创建不包含IDENTITY列的数据副本用于生成 INSERT SQL（IDENTITY列由数据库自动生成）
        Map<String, Object> dataWithoutId = new HashMap<>(data);
        if (identityColumnName != null) {
            // 移除IDENTITY列（不区分大小写）
            dataWithoutId.entrySet().removeIf(entry -> 
                entry.getKey().equalsIgnoreCase(identityColumnName));
        } else {
            // 如果没有找到IDENTITY列，尝试移除常见的ID字段名（不区分大小写）
            dataWithoutId.entrySet().removeIf(entry -> 
                entry.getKey().equalsIgnoreCase("id"));
        }

        // 生成 INSERT SQL（不包含IDENTITY字段）
        String insertSql = generateInsertSql(tableName, dataWithoutId, config);

        // 执行 INSERT 并获取自动生成的 id
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        Object generatedId = instance.execute(databaseTemplate -> {
            databaseTemplate.execute(insertSql);

            // 根据数据库类型获取自动生成的 id
            String driverClassName = config.getDriverClassName();
            String getIdentitySql;
            if (driverClassName != null && driverClassName.contains("sqlserver")) {
                // SQL Server 使用 SCOPE_IDENTITY()
                getIdentitySql = "SELECT SCOPE_IDENTITY()";
            } else if (driverClassName != null && driverClassName.contains("mysql")) {
                // MySQL 使用 LAST_INSERT_ID()
                getIdentitySql = "SELECT LAST_INSERT_ID()";
            } else {
                // 默认使用 SQL Server 语法
                getIdentitySql = "SELECT SCOPE_IDENTITY()";
            }

            return databaseTemplate.queryForObject(getIdentitySql, Object.class);
        });

        // 构建返回数据，包含自动生成的 id
        Map<String, Object> result = new HashMap<>(dataWithoutId);
        if (generatedId != null && identityColumnName != null) {
            // 将生成的 id 转换为合适的类型，使用实际的列名
            if (generatedId instanceof Number) {
                result.put(identityColumnName, ((Number) generatedId).intValue());
            } else {
                result.put(identityColumnName, generatedId);
            }
        } else if (generatedId != null) {
            // 如果没有找到IDENTITY列名，使用原始数据中的ID字段名（如果存在）
            String idKey = data.keySet().stream()
                .filter(key -> key.equalsIgnoreCase("id"))
                .findFirst()
                .orElse("id");
            if (generatedId instanceof Number) {
                result.put(idKey, ((Number) generatedId).intValue());
            } else {
                result.put(idKey, generatedId);
            }
        }

        return result;
    }

    /**
     * 查询表结构，找到IDENTITY/AUTO_INCREMENT列的名称
     * 
     * @param tableName 表名
     * @param config 数据库配置
     * @return IDENTITY/AUTO_INCREMENT列的名称，如果不存在则返回null
     */
    private String findIdentityColumnName(String tableName, DatabaseConfig config) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, true));
        }
        
        String connectorType = config.getConnectorType();
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
        
        return instance.execute(databaseTemplate -> {
            String sql;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                      "AND EXTRA LIKE '%auto_increment%' " +
                      "LIMIT 1";
                return databaseTemplate.queryForObject(sql, String.class, tableName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                      "AND COLUMNPROPERTY(OBJECT_ID(? + '.' + ?), COLUMN_NAME, 'IsIdentity') = 1";
                return databaseTemplate.queryForObject(sql, String.class, schema, tableName, schema, tableName);
            } else {
                // 其他数据库类型，返回null
                return null;
            }
        });
    }


    /**
     * 根据表名和数据生成 INSERT SQL
     *
     * @param tableName 表名
     * @param data      数据（Map<字段名, 值>）
     * @return INSERT SQL 语句
     */
    private String generateInsertSql(String tableName, Map<String, Object> data, DatabaseConfig config) {
        if (data == null || data.isEmpty()) {
            // 对于空数据，根据数据库类型生成不同的 SQL
            // SQL Server 不支持 DEFAULT VALUES，需要至少一个列
            // MySQL 支持 INSERT INTO table () VALUES ()，但需要至少一个列
            // 这里我们使用一个虚拟值来触发插入
            String driverClassName = config != null ? config.getDriverClassName() : null;
            if (driverClassName != null && driverClassName.contains("sqlserver")) {
                // SQL Server: 对于只有 IDENTITY 列的表，无法插入空行
                // 这种情况下，我们需要确保表至少有一个非 IDENTITY 列
                // 如果表只有 IDENTITY 列，这个调用应该失败，提示需要至少一个非主键列
                throw new IllegalArgumentException("SQL Server 不支持向只有 IDENTITY 列的表插入空数据。表 " + tableName + " 需要至少一个非主键列。");
            } else {
                // MySQL: 支持 INSERT INTO table () VALUES ()，但需要至少一个列
                // 如果表只有 AUTO_INCREMENT 主键，也需要至少一个非主键列
                throw new IllegalArgumentException("MySQL 不支持向只有 AUTO_INCREMENT 主键的表插入空数据。表 " + tableName + " 需要至少一个非主键列。");
            }
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!first) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append(entry.getKey());

            Object value = entry.getValue();
            if (value == null) {
                values.append("NULL");
            } else if (value instanceof String) {
                // 转义单引号
                String strValue = ((String) value).replace("'", "''");
                values.append("'").append(strValue).append("'");
            } else if (value instanceof Number) {
                values.append(value);
            } else if (value instanceof Boolean) {
                // 布尔类型：根据数据库类型转换为不同的值
                String driverClassName = config != null ? config.getDriverClassName() : null;
                if (driverClassName != null && driverClassName.contains("sqlserver")) {
                    // SQL Server: BIT 类型，使用 0 或 1
                    values.append(((Boolean) value) ? 1 : 0);
                } else {
                    // MySQL: TINYINT 类型，使用 0 或 1
                    values.append(((Boolean) value) ? 1 : 0);
                }
            } else if (value instanceof java.util.Date || value instanceof java.sql.Date || value instanceof java.sql.Timestamp) {
                // 日期时间类型：格式化为 SQL Server/MySQL 可识别的格式
                String driverClassName = config != null ? config.getDriverClassName() : null;
                java.util.Date dateValue;
                if (value instanceof java.sql.Date) {
                    dateValue = new java.util.Date(((java.sql.Date) value).getTime());
                } else if (value instanceof java.sql.Timestamp) {
                    dateValue = new java.util.Date(((java.sql.Timestamp) value).getTime());
                } else {
                    dateValue = (java.util.Date) value;
                }
                
                // 使用 SimpleDateFormat 格式化为数据库可识别的格式
                java.text.SimpleDateFormat sdf;
                if (driverClassName != null && driverClassName.contains("sqlserver")) {
                    // SQL Server: 使用 'YYYY-MM-DD HH:mm:ss' 格式
                    sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                } else {
                    // MySQL: 使用 'YYYY-MM-DD HH:mm:ss' 格式
                    sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }
                String formattedDate = sdf.format(dateValue);
                values.append("'").append(formattedDate).append("'");
            } else if (value instanceof java.time.LocalDate) {
                // Java 8+ LocalDate
                values.append("'").append(value.toString()).append("'");
            } else if (value instanceof java.time.LocalDateTime) {
                // Java 8+ LocalDateTime
                values.append("'").append(value.toString().replace("T", " ")).append("'");
            } else if (value instanceof java.time.OffsetDateTime) {
                // Java 8+ OffsetDateTime: 转换为 LocalDateTime（去掉时区信息）
                java.time.OffsetDateTime odt = (java.time.OffsetDateTime) value;
                values.append("'").append(odt.toLocalDateTime().toString().replace("T", " ")).append("'");
            } else {
                // 其他类型转为字符串
                values.append("'").append(value.toString().replace("'", "''")).append("'");
            }

            first = false;
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }

    /**
     * 等待数据同步完成（通过轮询检查目标表中是否存在数据）
     *
     * @param insertedData     插入的完整数据（Map<字段名, 值>）
     * @param tableName        表名
     * @param primaryKeyColumn 主键字段名（用于构建 WHERE 条件）
     * @param targetConfig     目标数据库配置
     * @param timeoutMs        超时时间（毫秒）
     */
    protected void waitForDataSync(Map<String, Object> insertedData, String tableName, String primaryKeyColumn, DatabaseConfig targetConfig, long timeoutMs) throws Exception {
        if (insertedData == null || insertedData.isEmpty()) {
            fail("插入的数据为空，无法验证");
        }

        Object primaryKeyValue = insertedData.get(primaryKeyColumn);
        if (primaryKeyValue == null) {
            fail("无法获取主键值进行验证");
        }

        long startTime = System.currentTimeMillis();
        long checkInterval = 300; // 每300ms检查一次

        logger.info("等待数据同步完成，表: {}, 主键: {}", tableName, primaryKeyValue);

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            // 查询目标表数据
            String whereCondition = primaryKeyColumn + " = " + (primaryKeyValue instanceof String ? "'" + primaryKeyValue + "'" : primaryKeyValue);
            String targetSql = String.format("SELECT * FROM %s WHERE %s", tableName, whereCondition);
            Map<String, Object> targetData = queryTableData(targetSql, targetConfig);

            if (!targetData.isEmpty()) {
                logger.info("数据同步完成，表: {}, 主键: {}", tableName, primaryKeyValue);
                Thread.sleep(200); // 再等待一小段时间，确保数据完全同步
                return;
            }

            Thread.sleep(checkInterval);
        }

        logger.warn("等待数据同步完成超时（{}ms），表: {}, 主键: {}", timeoutMs, tableName, primaryKeyValue);
    }

    /**
     * 验证插入的数据是否同步到目标表（直接使用插入的数据进行验证）
     *
     * @param insertedData     插入的完整数据（Map<字段名, 值>）
     * @param tableName        表名
     * @param primaryKeyColumn 主键字段名（用于构建 WHERE 条件）
     * @param targetConfig     目标数据库配置
     */
    protected void verifyDataSync(Map<String, Object> insertedData, String tableName, String primaryKeyColumn, DatabaseConfig targetConfig) throws Exception {
        if (insertedData == null || insertedData.isEmpty()) {
            fail("插入的数据为空，无法验证");
        }

        Object primaryKeyValue = insertedData.get(primaryKeyColumn);
        if (primaryKeyValue == null) {
            fail("无法获取主键值进行验证");
        }

        // 查询目标表数据
        String whereCondition = primaryKeyColumn + " = " + (primaryKeyValue instanceof String ? "'" + primaryKeyValue + "'" : primaryKeyValue);
        String targetSql = String.format("SELECT * FROM %s WHERE %s", tableName, whereCondition);
        Map<String, Object> targetData = queryTableData(targetSql, targetConfig);

        // 验证数据是否一致
        if (targetData.isEmpty()) {
            fail(String.format("目标表中未找到主键为 %s 的数据", primaryKeyValue));
        }

        for (Map.Entry<String, Object> entry : insertedData.entrySet()) {
            String fieldName = entry.getKey();
            Object sourceValue = entry.getValue();
            Object targetValue = targetData.get(fieldName);

            // 处理 NULL 值比较
            if (sourceValue == null && targetValue == null) {
                continue;
            }
            if (sourceValue == null || targetValue == null) {
                fail(String.format("字段 %s 的值不一致：源表=%s, 目标表=%s（主键: %s）",
                        fieldName, sourceValue, targetValue, primaryKeyValue));
            }

            // 比较值（处理数值类型和字符串）
            if (!compareValues(sourceValue, targetValue)) {
                fail(String.format("字段 %s 的值不一致：源表=%s, 目标表=%s（主键: %s）",
                        fieldName, sourceValue, targetValue, primaryKeyValue));
            }
        }

        logger.info("数据同步验证通过：表={}, 主键={}", tableName, primaryKeyValue);
    }

    /**
     * 查询表数据（返回第一行数据）
     */
    private Map<String, Object> queryTableData(String sql, DatabaseConfig config) throws Exception {
        DatabaseConnectorInstance instance = new DatabaseConnectorInstance(config);
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

    /**
     * 比较两个值是否相等（处理数值类型精度问题和字符串）
     */
    private boolean compareValues(Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }

        // 处理数值类型
        if (value1 instanceof Number && value2 instanceof Number) {
            return ((Number) value1).doubleValue() == ((Number) value2).doubleValue();
        }

        // 处理字符串（忽略大小写和前后空格）
        if (value1 instanceof String && value2 instanceof String) {
            return value1.toString().trim().equalsIgnoreCase(value2.toString().trim());
        }

        return value1.equals(value2);
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

    /**
     * 验证字段的默认值（支持 MySQL 和 SQL Server）
     * 
     * @param fieldName 字段名
     * @param tableName 表名
     * @param config 数据库配置
     * @param expectedDefault 期望的默认值（例如：'' 表示空字符串，'0' 表示0，null 表示无默认值）
     */
    protected void verifyFieldDefaultValue(String fieldName, String tableName, DatabaseConfig config, String expectedDefault) throws Exception {
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance = 
            (org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance) connectorFactory.connect(config);
        
        instance.execute(databaseTemplate -> {
            String sql;
            String actualDefault;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                // MySQL: 使用 INFORMATION_SCHEMA.COLUMNS
                sql = "SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualDefault = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                // SQL Server: 使用 INFORMATION_SCHEMA.COLUMNS，但需要指定 schema（通常是 dbo）
                // 先尝试获取当前 schema
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo"; // 默认 schema
                }
                
                sql = "SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualDefault = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            if (expectedDefault == null || "NULL".equalsIgnoreCase(expectedDefault)) {
                assertTrue(String.format("字段 %s 的默认值应为 NULL，但实际是 %s", fieldName, actualDefault),
                        actualDefault == null || "NULL".equalsIgnoreCase(actualDefault));
            } else {
                // 标准化比较：去除引号和空格，统一大小写
                String normalizedExpected = expectedDefault.replace("'", "").replace("\"", "").trim().toUpperCase();
                String normalizedActual = actualDefault != null ? 
                    actualDefault.replace("'", "").replace("\"", "").trim().toUpperCase() : "";
                
                // SQL Server 的默认值可能包含括号，例如 (N'') 或 ('')
                // 需要进一步处理
                normalizedActual = normalizedActual.replace("(", "").replace(")", "");
                normalizedExpected = normalizedExpected.replace("(", "").replace(")", "");
                
                // 对于空字符串，SQL Server 可能返回 N'' 或 ''，MySQL 可能返回 ''
                if (normalizedExpected.equals("") || normalizedExpected.equals("N")) {
                    assertTrue(String.format("字段 %s 的默认值应为空字符串，但实际是 %s", fieldName, actualDefault),
                            normalizedActual.equals("") || normalizedActual.equals("N"));
                } else {
                    // 尝试数值比较：对于数值类型，0 和 0.00 应该被认为是相等的
                    boolean matches = false;
                    try {
                        // 尝试将期望值和实际值都转换为数值进行比较
                        java.math.BigDecimal expectedNum = new java.math.BigDecimal(normalizedExpected);
                        java.math.BigDecimal actualNum = new java.math.BigDecimal(normalizedActual);
                        matches = expectedNum.compareTo(actualNum) == 0;
                    } catch (NumberFormatException e) {
                        // 如果转换失败，说明不是数值类型，使用字符串比较
                        matches = normalizedExpected.equals(normalizedActual);
                        
                        // 对于日期时间类型，SQL Server 可能自动补全时间部分
                        // 例如：'1900-01-01' 会被存储为 '1900-01-01 00:00:00'
                        // 这里我们检查实际值是否以期望值开头（对于日期类型）
                        if (!matches && normalizedExpected.contains("1900-01-01") && normalizedActual.contains("1900-01-01")) {
                            // 如果期望值是日期，实际值是日期时间，且日期部分相同，则认为匹配
                            matches = normalizedActual.startsWith(normalizedExpected);
                        }
                    }
                    assertTrue(String.format("字段 %s 的默认值应为 %s，但实际是 %s", fieldName, expectedDefault, actualDefault),
                            matches);
                }
            }
            
            logger.info("字段默认值验证通过: {} 的默认值是 {}", fieldName, actualDefault);
            return null;
        });
    }

    /**
     * 验证字段是否为NOT NULL（支持 MySQL 和 SQL Server）
     * 
     * @param fieldName 字段名
     * @param tableName 表名
     * @param config 数据库配置
     */
    protected void verifyFieldNotNull(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        // 确保 connectorType 已设置
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        @SuppressWarnings("unchecked")
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance = 
            (org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance) connectorFactory.connect(config);
        
        instance.execute(databaseTemplate -> {
            String sql;
            String isNullable;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                // MySQL: 使用 INFORMATION_SCHEMA.COLUMNS
                sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                isNullable = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                // SQL Server: 使用 INFORMATION_SCHEMA.COLUMNS，需要指定 schema
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo"; // 默认 schema
                }
                
                sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                isNullable = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertTrue(String.format("字段 %s 应为 NOT NULL，但实际 IS_NULLABLE = %s", fieldName, isNullable),
                    "NO".equalsIgnoreCase(isNullable));
            
            logger.info("字段NOT NULL约束验证通过: {}", fieldName);
            return null;
        });
    }

    /**
     * 验证表是否存在（支持 MySQL 和 SQL Server）
     */
    protected void verifyTableExists(String tableName, DatabaseConfig config) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            Integer count;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
                count = databaseTemplate.queryForObject(sql, Integer.class, tableName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
                count = databaseTemplate.queryForObject(sql, Integer.class, schema, tableName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertTrue(String.format("表 %s 应存在，但未找到", tableName), count != null && count > 0);
            logger.info("表存在验证通过: {}", tableName);
            return null;
        });
    }

    /**
     * 验证表的字段数量（支持 MySQL 和 SQL Server）
     */
    protected void verifyTableFieldCount(String tableName, DatabaseConfig config, int expectedCount) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            Integer actualCount;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
                actualCount = databaseTemplate.queryForObject(sql, Integer.class, tableName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
                actualCount = databaseTemplate.queryForObject(sql, Integer.class, schema, tableName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertNotNull(String.format("未找到表 %s", tableName), actualCount);
            assertEquals(String.format("表 %s 的字段数量应为 %d，但实际是 %d", tableName, expectedCount, actualCount),
                    Integer.valueOf(expectedCount), actualCount);
            logger.info("表字段数量验证通过: {} 有 {} 个字段", tableName, actualCount);
            return null;
        });
    }

    /**
     * 验证表的主键（支持 MySQL 和 SQL Server）
     */
    protected void verifyTablePrimaryKeys(String tableName, DatabaseConfig config, List<String> expectedKeys) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            List<String> actualKeys;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                      "AND CONSTRAINT_NAME = 'PRIMARY' " +
                      "ORDER BY ORDINAL_POSITION";
                actualKeys = databaseTemplate.queryForList(sql, String.class, tableName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                      "AND CONSTRAINT_NAME LIKE 'PK_%' " +
                      "ORDER BY ORDINAL_POSITION";
                actualKeys = databaseTemplate.queryForList(sql, String.class, schema, tableName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertNotNull(String.format("未找到表 %s 的主键信息", tableName), actualKeys);
            assertEquals(String.format("表 %s 的主键数量应为 %d，但实际是 %d", tableName, expectedKeys.size(), actualKeys.size()),
                    expectedKeys.size(), actualKeys.size());
            for (int i = 0; i < expectedKeys.size(); i++) {
                assertTrue(String.format("表 %s 的主键第 %d 列应为 %s，但实际是 %s", tableName, i + 1, expectedKeys.get(i), actualKeys.get(i)),
                        expectedKeys.get(i).equalsIgnoreCase(actualKeys.get(i)));
            }
            logger.info("表主键验证通过: {} 的主键是 {}", tableName, actualKeys);
            return null;
        });
    }

    /**
     * 验证字段类型（支持 MySQL 和 SQL Server）
     */
    protected void verifyFieldType(String fieldName, String tableName, DatabaseConfig config, String expectedType) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            String actualType;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualType = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualType = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertNotNull(String.format("未找到字段 %s", fieldName), actualType);
            assertTrue(String.format("字段 %s 的类型应为 %s，但实际是 %s", fieldName, expectedType, actualType),
                    expectedType.equalsIgnoreCase(actualType));
            logger.info("字段类型验证通过: {} 的类型是 {}", fieldName, actualType);
            return null;
        });
    }

    /**
     * 验证字段长度（支持 MySQL 和 SQL Server）
     */
    protected void verifyFieldLength(String fieldName, String tableName, DatabaseConfig config, int expectedLength) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            Integer actualLength;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualLength = databaseTemplate.queryForObject(sql, Integer.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualLength = databaseTemplate.queryForObject(sql, Integer.class, schema, tableName, fieldName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertNotNull(String.format("未找到字段 %s 或字段没有长度属性", fieldName), actualLength);
            assertEquals(String.format("字段 %s 的长度应为 %d，但实际是 %d", fieldName, expectedLength, actualLength),
                    Integer.valueOf(expectedLength), actualLength);
            logger.info("字段长度验证通过: {} 的长度是 {}", fieldName, actualLength);
            return null;
        });
    }

    /**
     * 验证字段是否可空（NULL）（支持 MySQL 和 SQL Server）
     */
    protected void verifyFieldNullable(String fieldName, String tableName, DatabaseConfig config) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String sql;
            String isNullable;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                isNullable = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                sql = "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                isNullable = databaseTemplate.queryForObject(sql, String.class, schema, tableName, fieldName);
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            assertNotNull(String.format("未找到字段 %s", fieldName), isNullable);
            assertEquals(String.format("字段 %s 应为NULL（可空），但实际是 %s", fieldName, isNullable),
                    "YES", isNullable.toUpperCase());
            logger.info("字段NULL约束验证通过: {} 是可空的", fieldName);
            return null;
        });
    }

    /**
     * 验证字段的COMMENT（支持 MySQL 和 SQL Server）
     * MySQL使用COLUMN_COMMENT，SQL Server使用MS_Description扩展属性
     */
    protected void verifyFieldComment(String fieldName, String tableName, DatabaseConfig config, String expectedComment) throws Exception {
        if (config.getConnectorType() == null) {
            config.setConnectorType(getConnectorType(config, false));
        }
        
        String connectorType = config.getConnectorType();
        org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance instance =
                new org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance(config);
        instance.execute(databaseTemplate -> {
            String actualComment;
            
            if ("mysql".equalsIgnoreCase(connectorType) || "Mysql".equals(connectorType)) {
                String sql = "SELECT COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                actualComment = databaseTemplate.queryForObject(sql, String.class, tableName, fieldName);
            } else if ("SqlServer".equals(connectorType) || "SqlServerCT".equals(connectorType)) {
                String schemaSql = "SELECT SCHEMA_NAME()";
                String schema = databaseTemplate.queryForObject(schemaSql, String.class);
                if (schema == null || schema.trim().isEmpty()) {
                    schema = "dbo";
                }
                String sql = "SELECT value FROM sys.extended_properties " +
                      "WHERE major_id = OBJECT_ID(? + '.' + ?) " +
                      "  AND minor_id = COLUMNPROPERTY(OBJECT_ID(? + '.' + ?), ?, 'ColumnId') " +
                      "  AND name = 'MS_Description'";
                actualComment = databaseTemplate.queryForObject(sql, String.class, schema, tableName, schema, tableName, fieldName);
                
                // SQL Server的sys.extended_properties.value可能返回转义后的单引号（''），需要转换为单个单引号（'）
                if (actualComment != null) {
                    actualComment = actualComment.replace("''", "'");
                }
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + connectorType);
            }
            
            String normalizedExpected = expectedComment != null ? expectedComment.trim() : "";
            String normalizedActual = actualComment != null ? actualComment.trim() : "";
            
            assertTrue(String.format("字段 %s 的COMMENT应为 '%s'，但实际是 '%s'", fieldName, expectedComment, normalizedActual),
                    normalizedExpected.equals(normalizedActual));
            logger.info("字段COMMENT验证通过: {} 的COMMENT是 '{}'", fieldName, normalizedActual);
            return null;
        });
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
                
                // 增强日志：输出当前所有字段映射，便于调试（每3秒输出一次）
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > 0 && elapsed % 3000 < checkInterval) {
                    List<String> currentFieldNames = tableGroup.getFieldMapping().stream()
                            .map(fm -> {
                                String sourceName = fm.getSource() != null ? fm.getSource().getName() : "null";
                                String targetName = fm.getTarget() != null ? fm.getTarget().getName() : "null";
                                return sourceName + "->" + targetName;
                            })
                            .collect(java.util.stream.Collectors.toList());
                    logger.info("等待中... 当前字段映射: {}", currentFieldNames);
                }
                
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

        // 超时时输出当前字段映射，便于调试
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mappingId);
        if (tableGroups != null && !tableGroups.isEmpty()) {
            TableGroup tableGroup = tableGroups.get(0);
            List<String> currentFieldNames = tableGroup.getFieldMapping().stream()
                    .map(fm -> {
                        String sourceName = fm.getSource() != null ? fm.getSource().getName() : "null";
                        String targetName = fm.getTarget() != null ? fm.getTarget().getName() : "null";
                        return sourceName + "->" + targetName;
                    })
                    .collect(java.util.stream.Collectors.toList());
            logger.warn("等待DDL处理完成超时（{}ms），字段: {}，当前字段映射: {}", timeoutMs, expectedFieldName, currentFieldNames);
        } else {
            logger.warn("等待DDL处理完成超时（{}ms），字段: {}，TableGroup列表为空", timeoutMs, expectedFieldName);
        }
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

