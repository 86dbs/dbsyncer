/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import com.alibaba.fastjson2.annotation.JSONField;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 整库迁移任务。
 * <p>库表关联配置存 {@code dbsyncer_table_group}；运行进度：库映射 status 摘要在任务级 Meta，
 * 表级快照在进度明细 Meta（{@code TASK_ID=table_group.id}）。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 11:19
 */
public class DatabaseSyncTask extends CommonTask {

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
     * 运行态执行快照：key = 库映射 index。不写入 dbsyncer_task.JSON；
     * 库 status 持久化到任务级 Meta，表级落到 table_group 进度 Meta。
     */
    @JSONField(serialize = false)
    private final ConcurrentHashMap<Integer, DatabaseSyncSnapshot> databaseSnapshots = new ConcurrentHashMap<>();

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

    public ConcurrentHashMap<Integer, DatabaseSyncSnapshot> getDatabaseSnapshots() {
        return databaseSnapshots;
    }

    public void putDatabaseSnapshot(Integer index, DatabaseSyncSnapshot snapshot) {
        if (index != null && snapshot != null) {
            databaseSnapshots.put(index, snapshot);
        }
    }

    public DatabaseSyncSnapshot getOrCreateDatabaseSnapshot(int mappingIndex) {
        return databaseSnapshots.computeIfAbsent(mappingIndex, key -> new DatabaseSyncSnapshot());
    }

    public DatabaseSyncTableSnapshot getTableSnapshot(int mappingIndex, int tableIndex) {
        DatabaseSyncSnapshot snapshot = databaseSnapshots.get(mappingIndex);
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
     * 新一次执行前重置进度快照（上次已 processed=已完成 时调用）。
     */
    public void resetRunSnapshots() {
        processed = 0;
        databaseSnapshots.clear();
    }
}
