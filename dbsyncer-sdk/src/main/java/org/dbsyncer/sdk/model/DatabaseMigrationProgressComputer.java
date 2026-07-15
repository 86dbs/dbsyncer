/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.enums.CommonTaskStepStatusEnum;
import org.dbsyncer.sdk.enums.DatabaseMigrationDetailTypeEnum;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 整库迁移任务进度计算（步骤计数法，稳定不漂移）
 *
 * @author wuji
 */
public final class DatabaseMigrationProgressComputer {

    private static final BigDecimal HUNDRED = new BigDecimal("100");

    /**
     * 计算进度百分比 0~100
     */
    public static BigDecimal calculateProgressPercent(DatabaseMigrationSyncTask task, int tableGroupSize) {
        if (task == null) {
            return null;
        }
        // 已标记完成
        if (task.getProcessed() != null && task.getProcessed() == 1) {
            return new BigDecimal("100.00");
        }
        //获取表的步数
        int stepsPerTable = getStepsPerTable(task);
        //数据库
        int totalDbSteps = task.getDatabaseMappings().size();
        int totalSteps = totalDbSteps + tableGroupSize * stepsPerTable;
        if (totalSteps <= 0) {
            return null;
        }

        //已完成的数据库结构
        long completedDbSteps = countCompletedDatabaseSteps(task);
        //已完成的表结构步骤
        long completedTableSteps = countCompletedTableSteps(task, stepsPerTable);
        long totalCompleted = completedDbSteps + completedTableSteps;

        // 防止溢出
        if (totalCompleted > totalSteps) {
            totalCompleted = totalSteps;
        }
        // 进度 = 完成数 / 总数 * 100
        return BigDecimal.valueOf(totalCompleted)
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(totalSteps), 2, RoundingMode.HALF_UP);
    }

    /**
     * 每张表包含的步骤数（结构1 + 数据1）
     */
    private static int getStepsPerTable(DatabaseMigrationSyncTask task) {
        int steps = 0;
        if (task.isEnableCopySchema()) steps++;
        if (task.isEnableCopyData()) steps++;
        return steps;
    }

    /**
     * 已完成的数据库步骤
     */
    private static long countCompletedDatabaseSteps(DatabaseMigrationSyncTask task) {
        ConcurrentHashMap<Integer, DatabaseMigrationSnapshot> snapshots = task.getDatabaseSnapshots();
        if (CollectionUtils.isEmpty(snapshots)) {
            return 0;
        }
        return snapshots.values().stream().filter(s -> s != null && CommonTaskStepStatusEnum.isDone(s.getStatus())).count();
    }

    /**
     * 已完成的表步骤（结构/数据）
     */
    private static long countCompletedTableSteps(DatabaseMigrationSyncTask task, int stepsPerTable) {
        ConcurrentHashMap<Integer, DatabaseMigrationSnapshot> snapshots = task.getDatabaseSnapshots();
        if (CollectionUtils.isEmpty(snapshots) || stepsPerTable <= 0) {
            return 0;
        }
        long count = 0;
        for (DatabaseMigrationSnapshot dbSnapshot : snapshots.values()) {
            if (dbSnapshot == null || CollectionUtils.isEmpty(dbSnapshot.getTables())) {
                continue;
            }
            for (DatabaseMigrationTableSnapshot tableSnapshot : dbSnapshot.getTables().values()) {
                if (tableSnapshot == null) {
                    continue;
                }
                if (task.isEnableCopySchema()
                        && DatabaseMigrationDetailTypeEnum.isSchemaPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
                    count++;
                }
                if (task.isEnableCopyData()
                        && DatabaseMigrationDetailTypeEnum.isDataPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 列表展示的已完成表数：与 {@link #calculateProgressPercent} 使用同一套步骤计数，
     * 换算为等效整表完成数（避免「进度 33% 但仍显示 0/65 张表」）。
     */
    public static int countCompletedTables(DatabaseMigrationSyncTask task, int totalTableCount) {
        if (task == null) {
            return 0;
        }
        if (task.getProcessed() != null && task.getProcessed() == 1 && totalTableCount > 0) {
            return totalTableCount;
        }
        int stepsPerTable = getStepsPerTable(task);
        if (stepsPerTable <= 0 || totalTableCount <= 0) {
            return 0;
        }
        long completedDbSteps = countCompletedDatabaseSteps(task);
        long completedTableSteps = countCompletedTableSteps(task, stepsPerTable);
        long totalCompletedSteps = completedDbSteps + completedTableSteps;
        int equivalentTables = (int) (totalCompletedSteps / stepsPerTable);
        return Math.min(equivalentTables, totalTableCount);
    }

}