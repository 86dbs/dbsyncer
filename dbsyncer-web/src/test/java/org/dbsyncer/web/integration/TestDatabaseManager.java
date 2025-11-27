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
     * @param sourceResetSql 重置源数据库的SQL脚本
     * @param targetResetSql 重置目标数据库的SQL脚本
     */
    public void resetTableStructure(String sourceResetSql, String targetResetSql) {
        logger.debug("开始重置测试数据库表结构");

        try {
            // 重置源数据库表结构
            executeSql(sourceConnectorInstance, sourceResetSql);

            // 重置目标数据库表结构
            executeSql(targetConnectorInstance, targetResetSql);

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

        connectorInstance.execute(databaseTemplate -> {
            try (Connection connection = databaseTemplate.getSimpleConnection().getConnection();
                 Statement statement = connection.createStatement()) {

                // 按分号分割SQL语句并逐个执行
                String[] sqlStatements = sql.split(";");
                for (String sqlStatement : sqlStatements) {
                    String trimmedSql = sqlStatement.trim();
                    if (!trimmedSql.isEmpty()) {
                        logger.debug("执行SQL: {}", trimmedSql);
                        try {
                            statement.execute(trimmedSql);
                        } catch (SQLException e) {
                            // 对于清理操作，某些语句可能失败（如表不存在），我们记录但不中断执行
                            logger.debug("SQL执行失败（可能可忽略）: {}", trimmedSql, e);
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
}





