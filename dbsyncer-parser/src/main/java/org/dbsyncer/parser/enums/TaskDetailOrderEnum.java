/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

/**
 * 任务明细列表显式排序（与指标无关时使用；有指标时优先用 {@link TaskDetailMetricEnum#getOrderSql()}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 16:00
 */
public enum TaskDetailOrderEnum {

    /**
     * 按明细更新时间
     */
    UPDATE_TIME("ORDER BY d.UPDATE_TIME DESC ");

    private final String sql;

    TaskDetailOrderEnum(String sql) {
        this.sql = sql;
    }

    /**
     * ORDER BY 子句（含尾部空格）。
     */
    public String getSql() {
        return sql;
    }
}
