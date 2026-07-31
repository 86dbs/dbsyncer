/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

/**
 * 任务明细列表状态筛选指标，并携带该指标对应的默认排序。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 16:00
 */
public enum TaskDetailMetricEnum {

    /**
     * 订正校验：按 DIFF（差异数）筛选与排序
     */
    DIFF("dm.DIFF", "ORDER BY dm.DIFF DESC, d.UPDATE_TIME DESC "),

    /**
     * 整库迁移：按 FAIL（失败数）筛选与排序
     */
    FAIL("dm.FAIL", "ORDER BY dm.FAIL DESC, d.UPDATE_TIME DESC ");

    private final String column;
    private final String orderSql;

    TaskDetailMetricEnum(String column, String orderSql) {
        this.column = column;
        this.orderSql = orderSql;
    }

    /**
     * SQL 列表达式（含表别名）。
     */
    public String getColumn() {
        return column;
    }

    /**
     * 该指标默认 ORDER BY 子句（含尾部空格）。
     */
    public String getOrderSql() {
        return orderSql;
    }
}
