/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.model;

import org.dbsyncer.sdk.model.CommonTaskSnapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * 整库迁移列表进度汇总：表明细快照 + 各表已同步行 / 源表总行（与快照同序）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-24
 */
public class TableProgressBundle {

    /** 各表明细 Meta 快照（与表 ID 列表顺序对齐，可含 null） */
    private final List<CommonTaskSnapshot> snapshots = new ArrayList<>();

    /** 各表已同步行（success+fail，与 snapshots 同序） */
    private final List<Long> syncedRowsPerTable = new ArrayList<>();

    /** 各表源端总行（无缓存为 0，与 snapshots 同序） */
    private final List<Long> sourceTotalPerTable = new ArrayList<>();

    public List<CommonTaskSnapshot> getSnapshots() {
        return snapshots;
    }

    public List<Long> getSyncedRowsPerTable() {
        return syncedRowsPerTable;
    }

    public List<Long> getSourceTotalPerTable() {
        return sourceTotalPerTable;
    }

    /**
     * 追加一张表的进度样本（三列表须同序追加）。
     */
    public void addTable(CommonTaskSnapshot snapshot, long syncedRows, long sourceTotal) {
        snapshots.add(snapshot);
        syncedRowsPerTable.add(Math.max(0L, syncedRows));
        sourceTotalPerTable.add(Math.max(0L, sourceTotal));
    }
}
