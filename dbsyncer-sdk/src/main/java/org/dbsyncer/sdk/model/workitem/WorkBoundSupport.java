/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.workitem;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.WorkBoundType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 执行侧工作项边界辅助：补全定位键等上下文。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-25
 */
public final class WorkBoundSupport {

    private WorkBoundSupport() {
    }

    /**
     * itemId 还原的边界常缺 pk，用源表主键补全（支持复合主键，逗号分隔）。
     *
     * @param bound       边界；可为 null
     * @param sourceTable 源表
     * @return 带 pk 的边界；整表或无法补全时原样返回
     */
    public static WorkBound enrichPk(WorkBound bound, Table sourceTable) {
        if (bound == null || StringUtil.isNotBlank(bound.getPk())) {
            return bound;
        }
        if (sourceTable == null || bound.getType() != WorkBoundType.CURSOR_BATCH) {
            return bound;
        }
        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(sourceTable.getColumn());
        if (CollectionUtils.isEmpty(pkFields) || !PrimaryKeyUtil.isSupportedCursor(pkFields)) {
            return bound;
        }
        String pkNames = pkFields.stream().map(Field::getName).collect(Collectors.joining(StringUtil.COMMA));
        return WorkBound.cursorBatch(bound.getItemId(), pkNames, bound.getFrom(), bound.getRowBudget());
    }
}
