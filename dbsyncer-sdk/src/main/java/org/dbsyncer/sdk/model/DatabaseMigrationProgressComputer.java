/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.MigrationStepStatusEnum;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 整库迁移任务进度计算（对齐订正校验 {@code ValidateSyncServiceImpl#calculateProgressPercent}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-29 11:19
 */
public final class DatabaseMigrationProgressComputer {

    private DatabaseMigrationProgressComputer() {
    }

    /**
     * @param task           迁移任务
     * @param tableGroupSize 参与迁移的表映射总数（TableGroup 条数）
     * @return 0~100；无有效步骤时返回 null
     */
    public static BigDecimal calculateProgressPercent(DatabaseMigrationSyncTask task, int tableGroupSize) {
        if (task == null) {
            return null;
        }
        if (task.getProcessed() != null && task.getProcessed() == 1) {
            return new BigDecimal("100.00");
        }

        int stepsPerTable = stepsPerTable(task);
        int databaseStepCount = countDatabaseSteps(task);
        int totalSteps = databaseStepCount + tableGroupSize * stepsPerTable;
        if (totalSteps <= 0) {
            return null;
        }

        long completed = 0;
        completed += countCompletedDatabaseSteps(task);
        completed += countCompletedTableSteps(task, stepsPerTable);

        if (completed > totalSteps) {
            completed = totalSteps;
        }
        return BigDecimal.valueOf(completed)
                .multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(totalSteps), 2, RoundingMode.HALF_UP);
    }

    private static int stepsPerTable(DatabaseMigrationSyncTask task) {
        int steps = 0;
        if (task.isEnableCopySchema()) {
            steps++;
        }
        if (task.isEnableCopyData()) {
            steps++;
        }
        return steps;
    }

    /**
     * 需创建/检查的目标库（命名空间）数量。
     */
    private static int countDatabaseSteps(DatabaseMigrationSyncTask task) {
        if (!task.isEnableCopySchema() || CollectionUtils.isEmpty(task.getDatabaseMappings())) {
            return 0;
        }
        int count = 0;
        for (DatabaseMapping mapping : task.getDatabaseMappings()) {
            if (mapping != null && StringUtil.isNotBlank(mapping.getTargetDatabase())) {
                count++;
            }
        }
        return count;
    }

    private static long countCompletedDatabaseSteps(DatabaseMigrationSyncTask task) {
        ConcurrentHashMap<Integer, DatabaseMigrationSnapshot> snapshots = task.getDatabaseSnapshots();
        if (CollectionUtils.isEmpty(snapshots)) {
            return 0;
        }
        int minIndex = snapshots.keySet().stream().min(Comparator.naturalOrder()).orElse(0);
        long doneInMap = snapshots.values().stream()
                .filter(s -> s != null && isStepDone(s.getStatus()))
                .count();
        return Math.max(0, minIndex - 1L) + doneInMap;
    }

    private static long countCompletedTableSteps(DatabaseMigrationSyncTask task, int stepsPerTable) {
        ConcurrentHashMap<Integer, DatabaseMigrationTableSnapshot> snapshots = task.getTableSnapshots();
        if (CollectionUtils.isEmpty(snapshots) || stepsPerTable == 0) {
            return 0;
        }
        int minIndex = snapshots.keySet().stream().min(Comparator.naturalOrder()).orElse(0);
        long doneInMap = 0;
        for (DatabaseMigrationTableSnapshot snapshot : snapshots.values()) {
            if (snapshot == null) {
                continue;
            }
            if (task.isEnableCopySchema() && isStepDone(snapshot.getSchemaStatus())) {
                doneInMap++;
            }
            if (task.isEnableCopyData() && isStepDone(snapshot.getDataStatus())) {
                doneInMap++;
            }
        }
        return Math.max(0, (long) (minIndex - 1) * stepsPerTable) + doneInMap;
    }

    private static boolean isStepDone(int status) {
        return MigrationStepStatusEnum.isDone(status);
    }
}
