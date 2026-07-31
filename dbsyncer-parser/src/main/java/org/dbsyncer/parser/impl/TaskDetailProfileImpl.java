/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.TaskDetailProfile;
import org.dbsyncer.parser.model.TaskDetailQuery;
import org.dbsyncer.parser.util.TaskDetailQuerySupport;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.storage.SqlQuery;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.TaskDetailUtil;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TaskDetailProfile} 实现：task_detail JOIN meta / table_group，存储侧分页。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-20 15:00
 */
@Component
public class TaskDetailProfileImpl implements TaskDetailProfile {

    private static final String SELECT_COLUMNS =
            "d.ID AS id, d.CREATE_TIME AS createTime, d.UPDATE_TIME AS updateTime, "
                    + "d.TABLE_GROUP_ID AS tableGroupId, d.TYPE AS type, d.TARGET_TABLE AS targetTable, "
                    + "d.IS_SUCCESS AS isSuccess, d.ERROR AS error, d.DATA AS data, "
                    + "tg.SOURCE_TABLE AS sourceTable, tg.TARGET_TABLE AS targetTableName, "
                    + "tg.SOURCE_DATABASE AS sourceDatabase, tg.TARGET_DATABASE AS targetDatabase, "
                    + "tg.SOURCE_SCHEMA AS sourceSchema, tg.TARGET_SCHEMA AS targetSchema, "
                    + "tg.SOURCE_TOTAL AS sourceTotal, tg.TARGET_TOTAL AS targetTotal, "
                    + "tg.SORT_INDEX AS sortIndex, "
                    + "dm.TOTAL AS total, dm.SUCCESS AS success, dm.FAIL AS fail, "
                    + "dm.DIFF AS diff, dm.FIXED AS fixed, dm.STATE AS state";

    private static final String FROM_JOIN =
            " FROM %s d "
                    + "INNER JOIN dbsyncer_meta dm ON dm.TASK_ID = d.TABLE_GROUP_ID AND dm.IS_TASK_DETAIL = 1 "
                    + "INNER JOIN dbsyncer_table_group tg ON tg.ID = d.TABLE_GROUP_ID ";

    /** 与 SELECT ... AS 别名一致，供 H2 等小写化后还原 */
    private static final String[] SQL_ALIASES = {
            "id", "createTime", "updateTime", "tableGroupId", "type", "targetTable",
            "isSuccess", "error", "data", "sourceTable", "targetTableName",
            "sourceDatabase", "targetDatabase", "sourceSchema", "targetSchema",
            "sourceTotal", "targetTotal", "sortIndex",
            "total", "success", "fail", "diff", "fixed", "state"
    };

    @Resource
    private StorageService storageService;

    @Override
    public Paging queryResults(TaskDetailQuery query) {
        Assert.notNull(query, "查询参数不能为空");
        String taskId = query.getTaskId();
        TaskDetailQuerySupport.assertTaskId(taskId);
        int pageNum = query.getPageNum();
        int pageSize = query.getPageSize();
        String detailTable = detailTableName(taskId);

        List<Object> args = new ArrayList<>();
        String where = TaskDetailQuerySupport.buildWhere(query, args);
        String orderSql = TaskDetailQuerySupport.resolveOrderSql(query);

        long total = queryCount(detailTable, where, args);
        Paging paging = new Paging(pageNum, pageSize);
        paging.setTotal(total);
        if (total <= 0) {
            paging.setData(Collections.emptyList());
            return paging;
        }

        String sql = "SELECT " + SELECT_COLUMNS + String.format(FROM_JOIN, detailTable) + where + orderSql;
        List<Map<String, Object>> rows = storageService.queryList(
                SqlQuery.of(sql, args.toArray()).page(pageNum, pageSize));
        List<Map<String, Object>> data = new ArrayList<>(rows == null ? 0 : rows.size());
        if (!CollectionUtils.isEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                data.add(toDisplayRow(row));
            }
        }
        paging.setData(data);
        return paging;
    }

    @Override
    public Map<String, Object> getDetail(TaskDetailQuery query) {
        Assert.notNull(query, "查询参数不能为空");
        String taskId = query.getTaskId();
        TaskDetailQuerySupport.assertTaskId(taskId);
        if (StringUtil.isBlank(query.getDetailId())) {
            return null;
        }
        String detailTable = detailTableName(taskId);
        List<Object> args = new ArrayList<>();
        String where = TaskDetailQuerySupport.buildWhere(query, args);
        String sql = "SELECT " + SELECT_COLUMNS + String.format(FROM_JOIN, detailTable) + where;
        List<Map<String, Object>> rows = storageService.queryList(SqlQuery.of(sql, args.toArray()).page(1, 1));
        if (CollectionUtils.isEmpty(rows)) {
            return null;
        }
        return toDisplayRow(rows.get(0));
    }

    private long queryCount(String detailTable, String where, List<Object> args) {
        String sql = "SELECT COUNT(1) AS cnt " + String.format(FROM_JOIN, detailTable) + where;
        List<Map<String, Object>> rows = storageService.queryList(SqlQuery.of(sql, args.toArray()));
        if (CollectionUtils.isEmpty(rows)) {
            return 0L;
        }
        Object cnt = rows.get(0).get("cnt");
        if (cnt == null) {
            cnt = rows.get(0).values().iterator().next();
        }
        return NumberUtil.toLong(String.valueOf(cnt));
    }

    private Map<String, Object> toDisplayRow(Map<String, Object> sqlRow) {
        // H2 等驱动可能把 AS sourceTable 变成 sourcetable，先规范化别名再装配前端字段
        Map<String, Object> row = normalizeSqlAliases(sqlRow);
        TaskDetailUtil.mergeDetailRow(row);

        Object sourceTable = row.get("sourceTable");
        Object targetTableName = row.get("targetTableName");
        Object targetTable = row.get(ConfigConstant.DETAIL_TARGET_TABLE);
        Object sourceDatabase = row.get(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE);
        Object sourceSchema = row.get(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA);
        Object targetDatabase = row.get(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE);
        Object targetSchema = row.get(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_SCHEMA);
        Object sourceTotal = row.get(ConfigConstant.TASK_SOURCE_TOTAL);
        Object targetTotal = row.get(ConfigConstant.TASK_TARGET_TOTAL);
        Object sortIndex = row.get(ConfigConstant.TABLE_GROUP_SORT_INDEX);
        Object total = row.get(ConfigConstant.META_TOTAL);
        Object success = row.get(ConfigConstant.META_SUCCESS);
        Object fail = row.get(ConfigConstant.META_FAIL);
        Object diff = row.get(ConfigConstant.META_DIFF);
        Object fixed = row.get(ConfigConstant.META_FIXED);
        Object state = row.get(ConfigConstant.META_STATE);

        putIfPresent(row, ConfigConstant.TASK_SOURCE_TABLE_NAME, sourceTable);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_TABLE, sourceTable);
        Object displayTargetTable = targetTableName != null ? targetTableName : targetTable;
        putIfPresent(row, ConfigConstant.DATA_TARGET_TABLE_NAME, displayTargetTable);
        putIfPresent(row, ConfigConstant.DETAIL_TARGET_TABLE, displayTargetTable);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_TABLE, displayTargetTable);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE, sourceDatabase);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA, sourceSchema);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE, targetDatabase);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_SCHEMA, targetSchema);
        putIfPresent(row, ConfigConstant.TASK_SOURCE_TOTAL, sourceTotal);
        putIfPresent(row, ConfigConstant.TASK_TARGET_TOTAL, targetTotal);
        putIfPresent(row, ConfigConstant.TABLE_GROUP_SORT_INDEX, sortIndex);
        putIfPresent(row, ConfigConstant.TASK_DIFF_TOTAL, diff);
        putIfPresent(row, ConfigConstant.TASK_FIXED_TOTAL, fixed);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL, success);
        putIfPresent(row, ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, fail);
        putIfPresent(row, ConfigConstant.META_TOTAL, total);
        putIfPresent(row, ConfigConstant.META_STATE, state);
        if (state != null) {
            row.put(ConfigConstant.TASK_STATUS, state);
        }
        return row;
    }

    /**
     * 将 SQL 结果别名规范为 camelCase，兼容 H2 等返回全小写 label 的驱动。
     */
    private Map<String, Object> normalizeSqlAliases(Map<String, Object> sqlRow) {
        Map<String, Object> row = new HashMap<>();
        if (sqlRow == null || sqlRow.isEmpty()) {
            return row;
        }
        row.putAll(sqlRow);
        for (String alias : SQL_ALIASES) {
            Object val = getIgnoreCase(sqlRow, alias);
            if (val != null) {
                row.put(alias, val);
            }
        }
        return row;
    }

    private static Object getIgnoreCase(Map<String, Object> row, String name) {
        if (row == null || StringUtil.isBlank(name)) {
            return null;
        }
        Object val = row.get(name);
        if (val != null) {
            return val;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && name.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void putIfPresent(Map<String, Object> row, String key, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String && StringUtil.isBlank((String) value)) {
            return;
        }
        row.put(key, value);
    }

    private String detailTableName(String taskId) {
        return "dbsyncer_task_detail_" + taskId;
    }
}
