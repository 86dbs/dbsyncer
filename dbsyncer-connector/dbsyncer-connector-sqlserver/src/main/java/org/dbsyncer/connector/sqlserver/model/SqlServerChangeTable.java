/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.model;

import org.dbsyncer.common.util.JsonUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SqlServerChangeTable {

    private String schemaName;
    private String tableName;
    private String captureInstance;
    private int changeTableObjectId;
    private byte[] startLsn;
    private byte[] stopLsn;
    private String capturedColumns;
    private List<String> capturedColumnList;
    private int capturedColumnsHash;

    public SqlServerChangeTable(String schemaName, String tableName, String captureInstance, int changeTableObjectId, byte[] startLsn, byte[] stopLsn, String capturedColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.captureInstance = captureInstance;
        this.capturedColumns = capturedColumns;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;

        // 解析 capturedColumns 为列表并计算 hash
        if (capturedColumns != null && !capturedColumns.trim().isEmpty()) {
            this.capturedColumnList = Arrays.stream(capturedColumns.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            // 计算 hash（用于快速比较，识别 sp_rename）
            this.capturedColumnsHash = calculateHash(this.capturedColumnList);
        } else {
            this.capturedColumnList = new ArrayList<>();
            this.capturedColumnsHash = 0;
        }
    }

    /**
     * 计算列列表的 hash 值（用于快速比较，识别 sp_rename）
     *
     * @param columnList 列名列表
     * @return hash 值
     */
    public static int calculateHash(List<String> columnList) {
        if (columnList == null || columnList.isEmpty()) {
            return 0;
        }
        // 使用列名列表的字符串表示来计算 hash
        // 对列名进行排序以确保顺序不影响 hash 值（用于检测列名变化）
        String sortedColumns = columnList.stream()
                .sorted()
                .collect(Collectors.joining(","));
        return sortedColumns.hashCode();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    public byte[] getStartLsn() {
        return startLsn;
    }

    public byte[] getStopLsn() {
        return stopLsn;
    }

    /**
     * 获取原始捕获列字符串（逗号分隔）
     *
     * @return 原始捕获列字符串
     */
    public String getCapturedColumns() {
        return capturedColumns;
    }

    /**
     * 获取已分割的捕获列列表
     *
     * @return 捕获列列表
     */
    public List<String> getCapturedColumnList() {
        return capturedColumnList;
    }

    /**
     * 获取捕获列的 hash 值（用于快速比较，识别 sp_rename）
     *
     * @return hash 值
     */
    public int getCapturedColumnsHash() {
        return capturedColumnsHash;
    }

    /**
     * 更新捕获列信息（用于在重新启用 CDC 后更新列列表）
     *
     * @param newCapturedColumns 新的捕获列字符串（逗号分隔）
     */
    public void updateCapturedColumns(String newCapturedColumns) {
        this.capturedColumns = newCapturedColumns;
        
        // 重新解析并计算 hash
        if (newCapturedColumns != null && !newCapturedColumns.trim().isEmpty()) {
            this.capturedColumnList = Arrays.stream(newCapturedColumns.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            this.capturedColumnsHash = calculateHash(this.capturedColumnList);
        } else {
            this.capturedColumnList = new ArrayList<>();
            this.capturedColumnsHash = 0;
        }
    }

    /**
     * 更新捕获列信息（用于在重新启用 CDC 后更新列列表）
     *
     * @param newCapturedColumnList 新的捕获列列表
     */
    public void updateCapturedColumns(List<String> newCapturedColumnList) {
        if (newCapturedColumnList == null || newCapturedColumnList.isEmpty()) {
            this.capturedColumns = "";
            this.capturedColumnList = new ArrayList<>();
            this.capturedColumnsHash = 0;
        } else {
            this.capturedColumnList = new ArrayList<>(newCapturedColumnList);
            this.capturedColumns = String.join(", ", newCapturedColumnList);
            this.capturedColumnsHash = calculateHash(this.capturedColumnList);
        }
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }

}
