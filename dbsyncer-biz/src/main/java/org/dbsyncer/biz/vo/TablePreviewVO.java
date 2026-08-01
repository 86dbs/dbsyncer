/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.common.model.CursorPageQuery;
import org.dbsyncer.sdk.model.Table;

import java.util.List;
import java.util.Map;

/**
 * 整库迁移预览表列表 VO（游标分页 + 表类型统计）
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-15 13:46
 */
public final class TablePreviewVO extends CursorPageQuery<Table> {

    /**
     * 各表类型数量统计，如 TABLE/VIEW
     */
    private Map<String, Integer> typeCounts;

    /**
     * 构建预览表游标分页 VO
     *
     * @param data     当前页表数据
     * @param total    总条数
     * @param cursor   游标（滚动分页标识）
     * @param pageSize 每页条数
     * @return 预览表 VO
     */
    public static TablePreviewVO of(List<Table> data, long total, String cursor, int pageSize) {
        TablePreviewVO vo = new TablePreviewVO();
        vo.fill(data, total, cursor, pageSize);
        return vo;
    }

    public Map<String, Integer> getTypeCounts() {
        return typeCounts;
    }

    public void setTypeCounts(Map<String, Integer> typeCounts) {
        this.typeCounts = typeCounts;
    }
}
