/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.enums.TaskDetailOrderEnum;
import org.dbsyncer.parser.enums.TaskDetailStatusEnum;
import org.dbsyncer.parser.model.TaskDetailQuery;
import org.springframework.util.Assert;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 任务明细查询 SQL 片段拼装（WHERE / ORDER BY / taskId 校验），便于单测。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 18:20
 */
public final class TaskDetailQuerySupport {

    private static final Pattern TASK_ID_PATTERN = Pattern.compile("^[0-9A-Za-z]+$");

    private TaskDetailQuerySupport() {
    }

    /**
     * 校验任务 ID（仅允许字母数字，防止分表名注入）。
     */
    public static void assertTaskId(String taskId) {
        if (StringUtil.isBlank(taskId) || !TASK_ID_PATTERN.matcher(taskId).matches()) {
            throw new ParserException("非法任务ID");
        }
    }

    /**
     * 解析 ORDER BY：显式 orderBy &gt; statusMetric 默认排序 &gt; UPDATE_TIME。
     */
    public static String resolveOrderSql(TaskDetailQuery query) {
        Assert.notNull(query, "查询参数不能为空");
        if (query.getOrderBy() != null) {
            return query.getOrderBy().getSql();
        }
        if (query.getStatusMetric() != null) {
            return query.getStatusMetric().getOrderSql();
        }
        return TaskDetailOrderEnum.UPDATE_TIME.getSql();
    }

    /**
     * 拼装 WHERE，并向 args 追加占位参数。
     */
    public static String buildWhere(TaskDetailQuery query, List<Object> args) {
        Assert.notNull(query, "查询参数不能为空");
        Assert.notNull(args, "参数列表不能为空");
        StringBuilder where = new StringBuilder("WHERE tg.TASK_ID = ? ");
        args.add(query.getTaskId());
        if (StringUtil.isNotBlank(query.getDetailId())) {
            where.append("AND d.ID = ? ");
            args.add(query.getDetailId());
        }
        if (StringUtil.isNotBlank(query.getDetailType())) {
            where.append("AND d.TYPE = ? ");
            args.add(query.getDetailType());
        }
        if (query.getDetailStatus() != null) {
            Assert.notNull(query.getStatusMetric(), "按状态筛选时 statusMetric 不能为空");
            String column = query.getStatusMetric().getColumn();
            if (query.getDetailStatus() == TaskDetailStatusEnum.SUCCESS) {
                where.append("AND ").append(column).append(" = 0 ");
            } else if (query.getDetailStatus() == TaskDetailStatusEnum.FAIL) {
                where.append("AND ").append(column).append(" > 0 ");
            }
        }
        return where.toString();
    }
}
