/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.bulk;

import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * SQL Server 批量复制工具类
 * 使用 VALUES 子句实现高效的批量插入，比 SQLServerBulkCopy 更简单直接
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025-01-XX
 */
public class SqlServerBulkCopyUtil {

    private static final Logger logger = LoggerFactory.getLogger(SqlServerBulkCopyUtil.class);
    private static final SqlServerTemplate sqlTemplate = new SqlServerTemplate();

    /**
     * 批次处理函数接口
     */
    @FunctionalInterface
    private interface BatchProcessor {
        int process(Connection connection, String sql, List<Field> fields, 
                   List<Map<String, Object>> batchData) throws SQLException;
    }

    /**
     * 统一的批次处理方法
     *
     * @param connection 数据库连接
     * @param tableName  目标表名
     * @param fields     字段列表
     * @param dataList   数据列表
     * @param schemaName schema名称
     * @param enableIdentityInsert 是否启用IDENTITY_INSERT
     * @param operationName 操作名称（用于日志）
     * @param processor 批次处理器
     * @return 处理的记录数
     * @throws SQLException SQL异常
     */
    private static int executeBatchOperation(Connection connection, String tableName,
                                           List<Field> fields, List<Map<String, Object>> dataList, 
                                           String schemaName, boolean enableIdentityInsert,
                                           String operationName, BatchProcessor processor) throws SQLException {
        if (dataList == null || dataList.isEmpty()) {
            return 0;
        }

        // SQL Server 参数限制：最多 2100 个参数
        // 每个字段一个参数，所以每批最多 2100 / 字段数 条记录
        int maxParamsPerBatch = 2100;
        int fieldsCount = fields.size();
        int maxRowsPerBatch = maxParamsPerBatch / fieldsCount;
        
        if (maxRowsPerBatch <= 0) {
            throw new SQLException("字段数量过多，无法进行批量" + operationName);
        }
        
        int totalProcessed = 0;
        int batchCount = (int) Math.ceil((double) dataList.size() / maxRowsPerBatch);
        
        logger.info("开始批量{}，总数据量: {}, 字段数: {}, 每批最大行数: {}, 批次数: {}", 
                   operationName, dataList.size(), fieldsCount, maxRowsPerBatch, batchCount);

        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
            int startIndex = batchIndex * maxRowsPerBatch;
            int endIndex = Math.min(startIndex + maxRowsPerBatch, dataList.size());
            List<Map<String, Object>> batchData = dataList.subList(startIndex, endIndex);
            
            try {
                int batchProcessed = executeBatchWithIdentityInsert(connection, tableName, fields, batchData, schemaName, enableIdentityInsert, processor);
                totalProcessed += batchProcessed;
                logger.debug("{} 批次 {}/{} 完成，处理 {} 条记录", operationName, batchIndex + 1, batchCount, batchProcessed);
            } catch (Exception e) {
                logger.error("{} 批次 {}/{} 失败: {}", operationName, batchIndex + 1, batchCount, e.getMessage(), e);
                throw new SQLException("批次" + operationName + "失败: " + e.getMessage(), e);
            }
        }

        logger.info("批量{}完成，共处理 {} 条记录到表 {}", operationName, totalProcessed, tableName);
        return totalProcessed;
    }

    /**
     * 执行单个批次，处理IDENTITY_INSERT逻辑
     *
     * @param connection 数据库连接
     * @param tableName  目标表名
     * @param fields     字段列表
     * @param batchData  批次数据
     * @param schemaName schema名称
     * @param enableIdentityInsert 是否启用IDENTITY_INSERT
     * @param processor  批次处理器
     * @return 处理的记录数
     * @throws SQLException SQL异常
     */
    private static int executeBatchWithIdentityInsert(Connection connection, String tableName,
                                                     List<Field> fields, List<Map<String, Object>> batchData,
                                                     String schemaName, boolean enableIdentityInsert,
                                                     BatchProcessor processor) throws SQLException {
        if (enableIdentityInsert) {
            // 需要启用 IDENTITY_INSERT 的情况
            String schemaTable = sqlTemplate.buildTable(schemaName, tableName);
            String identityInsertOn = sqlTemplate.buildIdentityInsertSql(schemaTable, true);
            String identityInsertOff = sqlTemplate.buildIdentityInsertSql(schemaTable, false);
            
            // 使用 Statement 执行 SET 语句，确保在同一个会话中
            try (java.sql.Statement stmt = connection.createStatement()) {
                // 1. 开启 IDENTITY_INSERT
                stmt.executeUpdate(identityInsertOn);
                
                // 2. 执行具体的批次操作
                int result = processor.process(connection, schemaTable, fields, batchData);
                
                // 3. 关闭 IDENTITY_INSERT
                stmt.executeUpdate(identityInsertOff);
                
                return result;
            } catch (Exception e) {
                // 确保在异常情况下也关闭 IDENTITY_INSERT
                try (java.sql.Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(identityInsertOff);
                } catch (SQLException closeException) {
                    logger.warn("关闭 IDENTITY_INSERT 时出错: {}", closeException.getMessage());
                }
                throw e;
            }
        } else {
            // 不需要 IDENTITY_INSERT 的情况，直接执行
            String schemaTable = sqlTemplate.buildTable(schemaName, tableName);
            return processor.process(connection, schemaTable, fields, batchData);
        }
    }

    /**
     * 执行批量插入
     *
     * @param connection 数据库连接
     * @param tableName  目标表名
     * @param fields     字段列表
     * @param dataList   数据列表
     * @param schemaName schema名称
     * @param enableIdentityInsert 是否启用IDENTITY_INSERT
     * @return 插入的记录数
     * @throws SQLException SQL异常
     */
    public static int bulkInsert(Connection connection, String tableName,
                                 List<Field> fields, List<Map<String, Object>> dataList, String schemaName, boolean enableIdentityInsert) throws SQLException {
        return executeBatchOperation(connection, tableName, fields, dataList, schemaName, enableIdentityInsert, 
                "插入", SqlServerBulkCopyUtil::executeInsertBatch);
    }
    
    
    /**
     * 执行插入批次（简化版，不处理IDENTITY_INSERT）
     */
    private static int executeInsertBatch(Connection connection, String schemaTable,
                                         List<Field> fields, List<Map<String, Object>> batchData) throws SQLException {
        if (batchData.isEmpty()) {
            return 0;
        }

        // 构建参数化的批量插入 SQL
        String sql = sqlTemplate.buildBatchInsertSql(schemaTable, fields, batchData.size());
        
        // 执行批量插入
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // 设置参数值
            int paramIndex = 1;
            for (Map<String, Object> rowData : batchData) {
                for (Field field : fields) {
                    Object value = rowData.get(field.getName());
                    ps.setObject(paramIndex++, value);
                }
            }
            return ps.executeUpdate();
        }
    }

    
    
    

    /**
     * 使用 MERGE 语句实现批量 UPSERT
     *
     * @param connection  数据库连接
     * @param tableName   目标表名
     * @param fields      字段列表
     * @param dataList    数据列表
     * @param primaryKeys 主键字段列表
     * @param schemaName  schema名称
     * @param enableIdentityInsert 是否启用IDENTITY_INSERT
     * @return 处理的记录数
     * @throws SQLException SQL异常
     */
    public static int bulkUpsert(Connection connection, String tableName,
                                 List<Field> fields, List<Map<String, Object>> dataList,
                                 List<String> primaryKeys, String schemaName, boolean enableIdentityInsert) throws SQLException {
        return executeBatchOperation(connection, tableName, fields, dataList, schemaName, enableIdentityInsert, 
                "UPSERT", (conn, schemaTable, flds, batchData) -> 
                    executeUpsertBatch(conn, schemaTable, flds, batchData, primaryKeys));
    }
    
    /**
     * 执行UPSERT批次（简化版，不处理IDENTITY_INSERT）
     */
    private static int executeUpsertBatch(Connection connection, String schemaTable,
                                         List<Field> fields, List<Map<String, Object>> batchData,
                                         List<String> primaryKeys) throws SQLException {
        if (batchData.isEmpty()) {
            return 0;
        }

        // 构建 MERGE 语句
        String sql = sqlTemplate.buildBatchUpsertSql(schemaTable, fields, batchData.size(), primaryKeys);
        
        // 执行批量 UPSERT
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // 设置参数
            int paramIndex = 1;
            for (Map<String, Object> rowData : batchData) {
                for (Field field : fields) {
                    Object value = rowData.get(field.getName());
                    ps.setObject(paramIndex++, value);
                }
            }

            return ps.executeUpdate();
        }
    }

}