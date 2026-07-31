/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.util;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.enums.DatabaseMigrationDetailTypeEnum;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.model.DatabaseSyncTask;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * 整库迁移任务进度计算（步骤计数法，基于 Meta 快照，稳定不漂移）。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class DatabaseSyncProgressUtil {

    private static final BigDecimal HUNDRED = new BigDecimal("100");

    private DatabaseSyncProgressUtil() {
    }

    /**
     * 计算进度百分比 0~100。
     *
     * @param task                 任务配置（开关）
     * @param tableGroupSize       表映射总数
     * @param mappingCount         库映射数
     * @param roundDone            任务级 Meta 是否本轮已完成（STATE=DONE）
     * @param mappingStatusByIndex 任务级 Meta 库映射 status 摘要
     * @param tableSnapshots       各表明细 Meta 快照（可含 null）
     */
    public static BigDecimal calculateProgressPercent(DatabaseSyncTask task, int tableGroupSize, int mappingCount,
                                                      boolean roundDone,
                                                      Map<Integer, Integer> mappingStatusByIndex,
                                                      List<CommonTaskSnapshot> tableSnapshots) {
        if (task == null) {
            return null;
        }
        if (roundDone) {
            return new BigDecimal("100.00");
        }
        int stepsPerTable = getStepsPerTable(task);
        int totalSteps = mappingCount + tableGroupSize * stepsPerTable;
        if (totalSteps <= 0) {
            return null;
        }
        long completedDbSteps = countCompletedDatabaseSteps(mappingStatusByIndex);
        long completedTableSteps = countCompletedTableSteps(task, tableSnapshots);
        long totalCompleted = completedDbSteps + completedTableSteps;
        if (totalCompleted > totalSteps) {
            totalCompleted = totalSteps;
        }
        return BigDecimal.valueOf(totalCompleted)
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(totalSteps), 2, RoundingMode.HALF_UP);
    }

    /**
     * 列表展示的已完成表数（等效整表完成数）。
     */
    public static int countCompletedTables(DatabaseSyncTask task, int totalTableCount, boolean roundDone,
                                           Map<Integer, Integer> mappingStatusByIndex,
                                           List<CommonTaskSnapshot> tableSnapshots) {
        if (task == null) {
            return 0;
        }
        if (roundDone && totalTableCount > 0) {
            return totalTableCount;
        }
        int stepsPerTable = getStepsPerTable(task);
        if (stepsPerTable <= 0 || totalTableCount <= 0) {
            return 0;
        }
        long totalCompletedSteps = countCompletedDatabaseSteps(mappingStatusByIndex)
                + countCompletedTableSteps(task, tableSnapshots);
        int equivalentTables = (int) (totalCompletedSteps / stepsPerTable);
        return Math.min(equivalentTables, totalTableCount);
    }

    private static int getStepsPerTable(DatabaseSyncTask task) {
        int steps = 0;
        if (task.isEnableCopySchema()) {
            steps++;
        }
        if (task.isEnableCopyData()) {
            steps++;
        }
        return steps;
    }

    private static long countCompletedDatabaseSteps(Map<Integer, Integer> mappingStatusByIndex) {
        if (CollectionUtils.isEmpty(mappingStatusByIndex)) {
            return 0;
        }
        return mappingStatusByIndex.values().stream()
                .filter(CommonTaskStatusEnum::isDone)
                .count();
    }

    private static long countCompletedTableSteps(DatabaseSyncTask task, List<CommonTaskSnapshot> tableSnapshots) {
        if (CollectionUtils.isEmpty(tableSnapshots)) {
            return 0;
        }
        long count = 0;
        for (CommonTaskSnapshot tableSnapshot : tableSnapshots) {
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
        return count;
    }

    /**
     * 从任务级 Meta.SNAPSHOT 解析库映射 status。
     */
    public static Map<Integer, Integer> readMappingStatus(Map<String, String> taskMetaSnapshot) {
        return TaskSnapshotUtil.readMappingStatusCodes(taskMetaSnapshot);
    }

    /**
     * 任务级 Meta.state 是否为本轮已完成（与 CommonTaskStatusEnum.DONE / MetaEnum.DONE 同码）。
     */
    public static boolean isRoundDone(Integer metaState) {
        return CommonTaskStatusEnum.isDone(metaState);
    }
}
