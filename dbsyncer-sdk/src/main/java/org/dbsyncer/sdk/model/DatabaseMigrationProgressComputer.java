/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.enums.MigrationStepStatusEnum;

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
        return snapshots.values().stream().filter(s -> s != null && MigrationStepStatusEnum.isDone(s.getStatus())).count();
    }

    /**
     * 已完成的表步骤（结构/数据）
     */
    private static long countCompletedTableSteps(DatabaseMigrationSyncTask task, int stepsPerTable) {
        ConcurrentHashMap<Integer, DatabaseMigrationTableSnapshot> snapshots = task.getTableSnapshots();
        if (CollectionUtils.isEmpty(snapshots) || stepsPerTable <= 0) {
            return 0;
        }
        long count = 0;
        for (DatabaseMigrationTableSnapshot snapshot : snapshots.values()) {
            if (snapshot == null) continue;
            //开启表结构
            if (task.isEnableCopySchema() && MigrationStepStatusEnum.isDone(snapshot.getSchemaStatus())) {
                count++;
            }
            //开启数据复制
            if (task.isEnableCopyData() && MigrationStepStatusEnum.isDone(snapshot.getDataStatus())) {
                count++;
            }
        }
        return count;
    }

}