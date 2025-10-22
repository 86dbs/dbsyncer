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
        if (dataList == null || dataList.isEmpty()) {
            return 0;
        }

        // SQL Server 参数限制：最多 2100 个参数
        // 每个字段一个参数，所以每批最多 2100 / 字段数 条记录
        int maxParamsPerBatch = 2100;
        int fieldsCount = fields.size();
        int maxRowsPerBatch = maxParamsPerBatch / fieldsCount;
        
        if (maxRowsPerBatch <= 0) {
            throw new SQLException("字段数量过多，无法进行批量插入");
        }
        
        int totalInserted = 0;
        int batchCount = (int) Math.ceil((double) dataList.size() / maxRowsPerBatch);
        
        logger.info("开始批量插入，总数据量: {}, 每批最大行数: {}, 批次数: {}", 
                   dataList.size(), maxRowsPerBatch, batchCount);

        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
            int startIndex = batchIndex * maxRowsPerBatch;
            int endIndex = Math.min(startIndex + maxRowsPerBatch, dataList.size());
            List<Map<String, Object>> batchData = dataList.subList(startIndex, endIndex);
            
            try {
                int batchInserted = insertBatchParameterized(connection, tableName, fields, batchData, schemaName, enableIdentityInsert);
                totalInserted += batchInserted;
                logger.info("批次 {}/{} 完成，插入 {} 条记录", batchIndex + 1, batchCount, batchInserted);
            } catch (Exception e) {
                logger.error("批次 {}/{} 插入失败: {}", batchIndex + 1, batchCount, e.getMessage(), e);
                throw new SQLException("批次插入失败: " + e.getMessage(), e);
            }
        }

        logger.info("批量插入完成，共插入 {} 条记录到表 {}", totalInserted, tableName);
        return totalInserted;
    }
    
    
    /**
     * 插入单个批次的数据（参数化 SQL，安全可靠）
     */
    private static int insertBatchParameterized(Connection connection, String tableName,
                                                List<Field> fields, List<Map<String, Object>> batchData, String schemaName, boolean enableIdentityInsert) throws SQLException {
        if (batchData.isEmpty()) {
            return 0;
        }

        // 构建参数化的批量插入 SQL
        String schemaTable = sqlTemplate.buildTable(schemaName, tableName);
        String sql = sqlTemplate.buildBatchInsertSql(schemaTable, fields, batchData.size());
        
        if (enableIdentityInsert) {
            // 需要启用 IDENTITY_INSERT 的情况
            String identityInsertOn = sqlTemplate.buildIdentityInsertSql(schemaTable, true);
            String identityInsertOff = sqlTemplate.buildIdentityInsertSql(schemaTable, false);
            
            try {
                // 1. 开启 IDENTITY_INSERT
                try (PreparedStatement ps1 = connection.prepareStatement(identityInsertOn)) {
                    ps1.executeUpdate();
                }
                
                // 2. 执行批量插入
                try (PreparedStatement ps2 = connection.prepareStatement(sql)) {
                    // 设置参数值
                    int paramIndex = 1;
                    for (Map<String, Object> rowData : batchData) {
                        for (Field field : fields) {
                            Object value = rowData.get(field.getName());
                            ps2.setObject(paramIndex++, value);
                        }
                    }
                    int result = ps2.executeUpdate();
                    
                    // 3. 关闭 IDENTITY_INSERT
                    try (PreparedStatement ps3 = connection.prepareStatement(identityInsertOff)) {
                        ps3.executeUpdate();
                    }
                    
                    return result;
                }
            } catch (Exception e) {
                // 确保在异常情况下也关闭 IDENTITY_INSERT
                try (PreparedStatement ps = connection.prepareStatement(identityInsertOff)) {
                    ps.executeUpdate();
                } catch (SQLException closeException) {
                    logger.warn("关闭 IDENTITY_INSERT 时出错: {}", closeException.getMessage());
                }
                throw e;
            }
        } else {
            // 不需要 IDENTITY_INSERT 的情况，直接执行插入
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
        if (dataList == null || dataList.isEmpty()) {
            return 0;
        }

        // SQL Server 参数限制：最多 2100 个参数
        // 每个字段一个参数，所以每批最多 2100 / 字段数 条记录
        int maxParamsPerBatch = 2100;
        int fieldsCount = fields.size();
        int maxRowsPerBatch = maxParamsPerBatch / fieldsCount;
        
        if (maxRowsPerBatch <= 0) {
            throw new SQLException("字段数量过多，无法进行批量 UPSERT");
        }

        int totalProcessed = 0;
        int batchCount = (int) Math.ceil((double) dataList.size() / maxRowsPerBatch);
        
        logger.info("开始批量 UPSERT，总数据量: {}, 字段数: {}, 每批最大行数: {}, 批次数: {}", 
                   dataList.size(), fieldsCount, maxRowsPerBatch, batchCount);

        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
            int startIndex = batchIndex * maxRowsPerBatch;
            int endIndex = Math.min(startIndex + maxRowsPerBatch, dataList.size());
            List<Map<String, Object>> batchData = dataList.subList(startIndex, endIndex);
            
            try {
                int batchProcessed = upsertBatch(connection, tableName, fields, batchData, primaryKeys, schemaName, enableIdentityInsert);
                totalProcessed += batchProcessed;
                logger.debug("UPSERT 批次 {}/{} 完成，处理 {} 条记录", batchIndex + 1, batchCount, batchProcessed);
            } catch (Exception e) {
                logger.error("UPSERT 批次 {}/{} 失败: {}", batchIndex + 1, batchCount, e.getMessage(), e);
                throw new SQLException("批次 UPSERT 失败: " + e.getMessage(), e);
            }
        }

        logger.info("批量 UPSERT 完成，共处理 {} 条记录到表 {}", totalProcessed, tableName);
        return totalProcessed;
    }
    
    /**
     * 执行单个批次的 UPSERT
     */
    private static int upsertBatch(Connection connection, String tableName,
                                   List<Field> fields, List<Map<String, Object>> batchData,
                                   List<String> primaryKeys, String schemaName, boolean enableIdentityInsert) throws SQLException {
        if (batchData.isEmpty()) {
            return 0;
        }

        // 构建 MERGE 语句
        String schemaTable = sqlTemplate.buildTable(schemaName, tableName);
        String sql = sqlTemplate.buildBatchUpsertSql(schemaTable, fields, batchData.size(), primaryKeys);
        
        if (enableIdentityInsert) {
            // 需要启用 IDENTITY_INSERT 的情况
            String identityInsertOn = sqlTemplate.buildIdentityInsertSql(schemaTable, true);
            String identityInsertOff = sqlTemplate.buildIdentityInsertSql(schemaTable, false);
            sql = identityInsertOn + " " + sql + " " + identityInsertOff;
        }

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