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
 * 整库迁移任务进度：按组合模式计算（仅结构 / 仅数据 / 都同步）。
 * <p>有数据阶段时以行级（已同步行 / 源表总行）为主，结构阶段仍按表；都同步时结构与数据加权混合。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class DatabaseSyncProgressUtil {

    private static final BigDecimal HUNDRED = new BigDecimal("100");

    /**
     * 都同步时结构段权重（其余为数据行级）。
     */
    private static final BigDecimal SCHEMA_WEIGHT = new BigDecimal("0.10");

    /**
     * 都同步时数据段权重。
     */
    private static final BigDecimal DATA_WEIGHT = new BigDecimal("0.90");

    private DatabaseSyncProgressUtil() {
    }

    /**
     * 计算进度百分比 0~100。
     *
     * @param task                 任务配置（开关）
     * @param tableGroupSize       表映射总数
     * @param mappingCount         库映射数（兼容入参，当前不单独占权）
     * @param roundDone            任务级 Meta 是否本轮已完成（STATE=DONE）
     * @param mappingStatusByIndex 任务级 Meta 库映射 status 摘要（兼容入参）
     * @param tableSnapshots       各表明细 Meta 快照（可含 null）
     * @param syncedRows           已同步行合计（success+fail）
     * @param sourceTotal          源表总行合计（各表 sourceTotal 之和）
     */
    public static BigDecimal calculateProgressPercent(DatabaseSyncTask task, int tableGroupSize, int mappingCount,
                                                      boolean roundDone,
                                                      Map<Integer, Integer> mappingStatusByIndex,
                                                      List<CommonTaskSnapshot> tableSnapshots,
                                                      long syncedRows, long sourceTotal) {
        if (task == null) {
            return null;
        }
        if (roundDone) {
            return new BigDecimal("100.00");
        }
        boolean schema = task.isEnableCopySchema();
        boolean data = task.isEnableCopyData();
        if (!schema && !data) {
            return null;
        }
        if (schema && !data) {
            return ratioPercent(countSchemaDoneTables(tableSnapshots), tableGroupSize);
        }
        if (!schema) {
            return rowPercent(syncedRows, sourceTotal, tableSnapshots, tableGroupSize);
        }
        // 都同步：结构表级 + 数据行级
        BigDecimal schemaPart = ratio(countSchemaDoneTables(tableSnapshots), tableGroupSize);
        BigDecimal dataPart = rowRatio(syncedRows, sourceTotal, tableSnapshots, tableGroupSize);
        return schemaPart.multiply(SCHEMA_WEIGHT)
                .add(dataPart.multiply(DATA_WEIGHT))
                .multiply(HUNDRED)
                .setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 兼容旧调用：无行数时数据段按「数据阶段完成表 / 总表」退化。
     */
    public static BigDecimal calculateProgressPercent(DatabaseSyncTask task, int tableGroupSize, int mappingCount,
                                                      boolean roundDone,
                                                      Map<Integer, Integer> mappingStatusByIndex,
                                                      List<CommonTaskSnapshot> tableSnapshots) {
        return calculateProgressPercent(task, tableGroupSize, mappingCount, roundDone, mappingStatusByIndex,
                tableSnapshots, 0L, 0L);
    }

    /**
     * 列表展示的已完成表数：启用阶段均已完成的表。
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
        if (totalTableCount <= 0 || CollectionUtils.isEmpty(tableSnapshots)) {
            return 0;
        }
        int count = 0;
        for (CommonTaskSnapshot tableSnapshot : tableSnapshots) {
            if (isTableFullyDone(task, tableSnapshot)) {
                count++;
            }
        }
        return Math.min(count, totalTableCount);
    }

    private static BigDecimal rowPercent(long syncedRows, long sourceTotal,
                                         List<CommonTaskSnapshot> tableSnapshots, int tableGroupSize) {
        return rowRatio(syncedRows, sourceTotal, tableSnapshots, tableGroupSize)
                .multiply(HUNDRED)
                .setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 行级比例；无分母时退化为数据阶段完成表比例，避免长期为 0。
     */
    private static BigDecimal rowRatio(long syncedRows, long sourceTotal,
                                       List<CommonTaskSnapshot> tableSnapshots, int tableGroupSize) {
        if (sourceTotal > 0) {
            long capped = Math.min(Math.max(syncedRows, 0L), sourceTotal);
            return ratio(capped, sourceTotal);
        }
        return ratio(countDataDoneTables(tableSnapshots), tableGroupSize);
    }

    private static BigDecimal ratioPercent(long completed, long total) {
        return ratio(completed, total).multiply(HUNDRED).setScale(2, RoundingMode.HALF_UP);
    }

    private static BigDecimal ratio(long completed, long total) {
        if (total <= 0) {
            return BigDecimal.ZERO;
        }
        long capped = Math.min(Math.max(completed, 0L), total);
        return BigDecimal.valueOf(capped)
                .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP);
    }

    private static int countSchemaDoneTables(List<CommonTaskSnapshot> tableSnapshots) {
        if (CollectionUtils.isEmpty(tableSnapshots)) {
            return 0;
        }
        int count = 0;
        for (CommonTaskSnapshot tableSnapshot : tableSnapshots) {
            if (tableSnapshot != null
                    && DatabaseMigrationDetailTypeEnum.isSchemaPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
                count++;
            }
        }
        return count;
    }

    private static int countDataDoneTables(List<CommonTaskSnapshot> tableSnapshots) {
        if (CollectionUtils.isEmpty(tableSnapshots)) {
            return 0;
        }
        int count = 0;
        for (CommonTaskSnapshot tableSnapshot : tableSnapshots) {
            if (tableSnapshot != null
                    && DatabaseMigrationDetailTypeEnum.isDataPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
                count++;
            }
        }
        return count;
    }

    private static boolean isTableFullyDone(DatabaseSyncTask task, CommonTaskSnapshot tableSnapshot) {
        if (tableSnapshot == null) {
            return false;
        }
        if (task.isEnableCopySchema()
                && !DatabaseMigrationDetailTypeEnum.isSchemaPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
            return false;
        }
        if (task.isEnableCopyData()
                && !DatabaseMigrationDetailTypeEnum.isDataPhaseDone(tableSnapshot.getStep(), tableSnapshot.getStatus())) {
            return false;
        }
        return task.isEnableCopySchema() || task.isEnableCopyData();
    }

    /**
     * 从任务级 Meta.SNAPSHOT 解析库映射 status。
     */
    public static Map<Integer, Integer> readMappingStatus(Map<String, String> taskMetaSnapshot) {
        return TaskSnapshotUtil.readMappingStatusCodes(taskMetaSnapshot);
    }

    /**
     * 任务级 Meta.state 是否为本轮已完成（与 {@link org.dbsyncer.common.enums.CommonTaskStatusEnum#DONE} 同码）。
     */
    public static boolean isRoundDone(Integer metaState) {
        return CommonTaskStatusEnum.isDone(metaState);
    }
}
