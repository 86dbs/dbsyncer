package org.dbsyncer.web.integration;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 测试数据库管理器
 * 负责在集成测试前后创建和清理测试所需的数据库表和数据
 */
public class TestDatabaseManager {

    private static final Logger logger = LoggerFactory.getLogger(TestDatabaseManager.class);

    private DatabaseConnectorInstance sourceConnectorInstance;
    private DatabaseConnectorInstance targetConnectorInstance;

    public TestDatabaseManager(DatabaseConfig sourceConfig, DatabaseConfig targetConfig) {
        this.sourceConnectorInstance = new DatabaseConnectorInstance(sourceConfig);
        this.targetConnectorInstance = new DatabaseConnectorInstance(targetConfig);
    }

    /**
     * 初始化测试数据库环境
     *
     * @param sourceInitSql 初始化源数据库的SQL脚本
     * @param targetInitSql 初始化目标数据库的SQL脚本
     */
    public void initializeTestEnvironment(String sourceInitSql, String targetInitSql) {
        logger.info("开始初始化测试数据库环境");

        try {
            // 初始化源数据库
            executeSql(sourceConnectorInstance, sourceInitSql);

            // 初始化目标数据库
            executeSql(targetConnectorInstance, targetInitSql);

            logger.info("测试数据库环境初始化完成");
        } catch (Exception e) {
            logger.error("初始化测试数据库环境失败", e);
            throw new RuntimeException("初始化测试数据库环境失败", e);
        }
    }

    /**
     * 清理测试数据库环境
     *
     * @param sourceCleanupSql 清理源数据库的SQL脚本
     * @param targetCleanupSql 清理目标数据库的SQL脚本
     */
    public void cleanupTestEnvironment(String sourceCleanupSql, String targetCleanupSql) {
        logger.info("开始清理测试数据库环境");

        try {
            // 清理源数据库
            executeSql(sourceConnectorInstance, sourceCleanupSql);

            // 清理目标数据库
            executeSql(targetConnectorInstance, targetCleanupSql);

            logger.info("测试数据库环境清理完成");
        } catch (Exception e) {
            logger.error("清理测试数据库环境失败", e);
            // 不抛出异常，因为测试可能已经完成，清理失败不应影响测试结果
        }
    }

    /**
     * 重置测试数据库表结构到初始状态
     * 用于在测试之间恢复表结构，确保测试间的隔离性
     *
     * @param script SQL脚本（同时应用于源和目标数据库）
     */
    public void resetTableStructure(String script) {
        resetTableStructure(script, script);
    }

    /**
     * 重置测试数据库表结构到初始状态
     * 用于在测试之间恢复表结构，确保测试间的隔离性
     *
     * @param sourceScript 源数据库的SQL脚本
     * @param targetScript 目标数据库的SQL脚本
     */
    public void resetTableStructure(String sourceScript, String targetScript) {
        logger.debug("开始重置测试数据库表结构");

        try {
            executeSql(sourceConnectorInstance, sourceScript);
            executeSql(targetConnectorInstance, targetScript);

            logger.debug("测试数据库表结构重置完成");
        } catch (Exception e) {
            logger.error("重置测试数据库表结构失败", e);
            // 不抛出异常，避免影响测试结果，但记录错误
        }
    }

    /**
     * 执行SQL脚本
     *
     * @param connectorInstance 数据库连接实例
     * @param sql               要执行的SQL脚本
     */
    private void executeSql(DatabaseConnectorInstance connectorInstance, String sql) throws Exception {
        if (sql == null || sql.trim().isEmpty()) {
            return;
        }

        // 预处理SQL脚本：移除注释和空行，合并多行语句为单行
        String processedSql = preprocessSqlScript(sql);

        connectorInstance.execute(databaseTemplate -> {
            try (Connection connection = databaseTemplate.getSimpleConnection().getConnection();
                 Statement statement = connection.createStatement()) {

                // 按分号分割SQL语句并逐个执行
                String[] sqlStatements = processedSql.split(";");
                for (String sqlStatement : sqlStatements) {
                    String trimmedSql = sqlStatement.trim();
                    if (!trimmedSql.isEmpty()) {
                        logger.info("执行SQL: {}", trimmedSql);
                        try {
                            statement.execute(trimmedSql);
                        } catch (SQLException e) {
                            // 对于清理操作，某些语句可能失败（如表不存在），我们记录但不中断执行
                            logger.error("SQL执行失败（可能可忽略）: {}", trimmedSql, e);
                        }
                    }
                }

                return null;
            } catch (SQLException e) {
                logger.error("执行SQL失败: {}", sql, e);
                throw new RuntimeException("执行SQL失败", e);
            }
        });
    }

    /**
     * 预处理SQL脚本：移除注释行和空行，合并多行语句为单行
     * 这样可以让简单的 split(";") 正确处理多行SQL语句（如CREATE TABLE）
     *
     * @param sql 原始SQL脚本
     * @return 处理后的SQL脚本（单行格式，用空格分隔）
     */
    private String preprocessSqlScript(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        String[] lines = sql.split("\n");
        
        for (String line : lines) {
            String trimmed = line.trim();
            // 跳过空行和注释行（以--开头的行）
            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }
            // 移除行内注释（-- 后面的内容）
            int commentIndex = trimmed.indexOf("--");
            if (commentIndex >= 0) {
                trimmed = trimmed.substring(0, commentIndex).trim();
            }
            if (!trimmed.isEmpty()) {
                result.append(trimmed).append(" ");
            }
        }
        
        return result.toString().trim();
    }
}

