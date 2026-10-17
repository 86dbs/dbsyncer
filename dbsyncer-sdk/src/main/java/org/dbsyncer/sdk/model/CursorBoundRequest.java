/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.Map;

/**
 * 分片游标边界解析请求：按起始游标与行预算解析结束游标。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-04
 */
public class CursorBoundRequest {

    /**
     * 表映射已保存的执行命令（含 QUERY / QUERY_CURSOR / CURSOR_PK_NAMES 等）
     */
    private Map<String, String> command;

    /**
     * 源表（含字段与主键标记）
     */
    private Table sourceTable;

    /**
     * 起始游标（排他下界）；空串表示表头
     */
    private String startCursor = "";

    /**
     * 本片最大行数
     */
    private int budget;

    public Map<String, String> getCommand() {
        return command;
    }

    public void setCommand(Map<String, String> command) {
        this.command = command;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getStartCursor() {
        return startCursor;
    }

    public void setStartCursor(String startCursor) {
        this.startCursor = startCursor;
    }

    public int getBudget() {
        return budget;
    }

    public void setBudget(int budget) {
        this.budget = budget;
    }
}
