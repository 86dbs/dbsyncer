/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 整库迁移任务。
 * <p>进度快照：{@link #databaseSnapshots}（库级，内含表级结构/数据/行游标）；</p>
 * <p>列表进度由 {@link DatabaseMigrationProgressComputer#calculateProgressPercent} 计算。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:19
 */
public class DatabaseMigrationSyncTask extends CommonTask {

    /**
     * 库映射列表（源/目标连接器 ID 配置在每条 {@link DatabaseMapping} 上）
     */
    private List<DatabaseMapping> databaseMappings;

    /**
     * 是否复制表结构
     */
    private boolean enableCopySchema;
    /**
     * 表结构是否覆盖（目标已存在时）
     */
    private boolean overwriteSchema;
    /**
     * 是否同步数据
     */
    private boolean enableCopyData;
    /**
     * 数据是否覆盖（目标已存在时）
     */
    private boolean overwriteData;

    /**
     * 任务是否已全部处理完成：0-执行中，1-已结束（与订正校验一致，结束后可清空快照）
     */
    private Integer processed = 0;

    /**
     * 最近一次执行开始时间（毫秒时间戳）
     */
    private Long beginTime;

    /**
     * 最近一次执行结束时间（毫秒时间戳，执行完成后写入）
     */
    private Long endTime;

    /**
     * 执行快照：key = 库映射 index，value 含库流水线状态及下属表快照。TODO fan序列后是HashMap
     */
    private final ConcurrentHashMap<Integer, DatabaseMigrationSnapshot> databaseSnapshots = new ConcurrentHashMap<>();

    /**
     * 分页读取条数
     */
    private int readNum = 10000;

    /**
     * 单次写入条数
     */
    private int batchNum = 1000;

    /**
     * 表级并发线程数（预留，与订正校验 threadNum 一致）
     */
    private int threadNum = 5;

    public List<DatabaseMapping> getDatabaseMappings() {
        return databaseMappings;
    }

    public void setDatabaseMappings(List<DatabaseMapping> databaseMappings) {
        this.databaseMappings = databaseMappings;
    }

    /**
     * 按 index 升序返回库映射列表（副本，不修改原列表）。
     */
    public List<DatabaseMapping> getSortedDatabaseMappings() {
        if (databaseMappings == null || databaseMappings.isEmpty()) {
            return Collections.emptyList();
        }
        return databaseMappings.stream()
                .sorted(Comparator.comparingInt(DatabaseMapping::getIndex))
                .collect(Collectors.toList());
    }

    public boolean isEnableCopySchema() {
        return enableCopySchema;
    }

    public void setEnableCopySchema(boolean enableCopySchema) {
        this.enableCopySchema = enableCopySchema;
    }

    public boolean isOverwriteSchema() {
        return overwriteSchema;
    }

    public void setOverwriteSchema(boolean overwriteSchema) {
        this.overwriteSchema = overwriteSchema;
    }

    public boolean isEnableCopyData() {
        return enableCopyData;
    }

    public void setEnableCopyData(boolean enableCopyData) {
        this.enableCopyData = enableCopyData;
    }

    public boolean isOverwriteData() {
        return overwriteData;
    }

    public void setOverwriteData(boolean overwriteData) {
        this.overwriteData = overwriteData;
    }

    public Integer getProcessed() {
        return processed;
    }

    public void setProcessed(Integer processed) {
        this.processed = processed;
    }

    public Long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Long beginTime) {
        this.beginTime = beginTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public ConcurrentHashMap<Integer, DatabaseMigrationSnapshot> getDatabaseSnapshots() {
        return databaseSnapshots;
    }

    public void putDatabaseSnapshot(Integer index, DatabaseMigrationSnapshot snapshot) {
        if (index != null && snapshot != null) {
            databaseSnapshots.put(index, snapshot);
        }
    }

    public DatabaseMigrationSnapshot getOrCreateDatabaseSnapshot(int mappingIndex) {
        return databaseSnapshots.computeIfAbsent(mappingIndex, key -> new DatabaseMigrationSnapshot());
    }

    public DatabaseMigrationTableSnapshot getTableSnapshot(int mappingIndex, int tableIndex) {
        DatabaseMigrationSnapshot snapshot = databaseSnapshots.get(mappingIndex);
        return snapshot == null ? null : snapshot.getTable(tableIndex);
    }

    public int getReadNum() {
        return readNum;
    }

    public void setReadNum(int readNum) {
        this.readNum = readNum;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    /**
     * 新一次执行前重置进度快照（上次已 processed=1 时调用）。
     */
    public void resetRunSnapshots() {
        processed = 0;
        databaseSnapshots.clear();
    }
}
