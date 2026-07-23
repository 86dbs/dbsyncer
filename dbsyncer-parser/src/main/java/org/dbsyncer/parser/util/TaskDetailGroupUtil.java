/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.util.TaskDetailUtil;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 校验/迁移结果：task_detail JOIN meta，关联列来自 table_group。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-20 15:00
 */
@Component
public class TaskDetailGroupUtil {

    /**
     * 明细分表分页拉取每页条数（需全量时循环翻页累加）
     */
    private static final int DETAIL_FETCH_PAGE_SIZE = 1000;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private StorageService storageService;

    /**
     * 查询校验/迁移结果列表(连表装配后应用侧过滤/排序/分页)。
     *
     * @param taskId         任务 ID
     * @param filter         行过滤(可空)
     * @param comparator     排序(可空)
     * @param pageNum        页码
     * @param pageSize       页大小
     * @param detailType     明细 TYPE 过滤(可空)
     * @return 分页结果
     */
    public Paging queryJoinedResults(String taskId, Predicate<Map<String, Object>> filter,
                                     Comparator<Map<String, Object>> comparator,
                                     int pageNum, int pageSize, String detailType) {
        Map<String, Map<String, Object>> tableGroupDisplay = buildTableGroupDisplayMap(taskId);
        List<Map<String, Object>> detailRows = queryDetailRows(taskId, detailType);
        if (CollectionUtils.isEmpty(detailRows)) {
            return TaskDetailUtil.pageDetails(null, filter, comparator, pageNum, pageSize);
        }
        List<String> tableGroupIds = detailRows.stream()
                .map(row -> row.get(ConfigConstant.DATA_TABLE_GROUP_ID) == null
                        ? null : String.valueOf(row.get(ConfigConstant.DATA_TABLE_GROUP_ID)))
                .filter(StringUtil::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
        Map<String, Meta> metaMap = profileComponent.getDetailMetaMap(tableGroupIds);

        List<Map<String, Object>> joined = new ArrayList<>(detailRows.size());
        for (Map<String, Object> detailRow : detailRows) {
            String tableGroupId = detailRow.get(ConfigConstant.DATA_TABLE_GROUP_ID) == null
                    ? null : String.valueOf(detailRow.get(ConfigConstant.DATA_TABLE_GROUP_ID));
            Map<String, Object> tgRow = tableGroupId == null ? null : tableGroupDisplay.get(tableGroupId);
            Meta meta = tableGroupId == null ? null : metaMap.get(tableGroupId);
            Map<String, Object> metaRow = meta == null ? null : TaskDetailUtil.toMetaDisplayMap(
                    meta.getTotal() == null ? 0L : meta.getTotal().get(),
                    meta.getSuccess() == null ? 0L : meta.getSuccess().get(),
                    meta.getFail() == null ? 0L : meta.getFail().get(),
                    meta.getDiff() == null ? 0L : meta.getDiff().get(),
                    meta.getFixed() == null ? 0L : meta.getFixed().get(),
                    meta.getState());
            joined.add(TaskDetailUtil.assembleJoinedRow(detailRow, tgRow, metaRow));
        }
        return TaskDetailUtil.pageDetails(joined, filter, comparator, pageNum, pageSize);
    }

    /**
     * 单条明细连表查询。
     *
     * @param taskId   任务 ID
     * @param detailId 明细 ID
     * @return 展示行，不存在时 null
     */
    public Map<String, Object> getJoinedDetail(String taskId, String detailId) {
        if (StringUtil.isBlank(taskId) || StringUtil.isBlank(detailId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.TASK_DETAIL);
        query.setMetaId(taskId);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, detailId);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object row = paging.getData().iterator().next();
        if (!(row instanceof Map)) {
            return null;
        }
        Map<String, Object> detailRow = new HashMap<>((Map<String, Object>) row);
        String tableGroupId = detailRow.get(ConfigConstant.DATA_TABLE_GROUP_ID) == null
                ? null : String.valueOf(detailRow.get(ConfigConstant.DATA_TABLE_GROUP_ID));
        Map<String, Object> tgRow = tableGroupId == null ? null : buildTableGroupDisplayMap(taskId).get(tableGroupId);
        Meta meta = StringUtil.isBlank(tableGroupId) ? null : profileComponent.getMetaByTaskId(tableGroupId, 1);
        Map<String, Object> metaRow = meta == null ? null : org.dbsyncer.sdk.util.TaskDetailUtil.toMetaDisplayMap(
                meta.getTotal() == null ? 0L : meta.getTotal().get(),
                meta.getSuccess() == null ? 0L : meta.getSuccess().get(),
                meta.getFail() == null ? 0L : meta.getFail().get(),
                meta.getDiff() == null ? 0L : meta.getDiff().get(),
                meta.getFixed() == null ? 0L : meta.getFixed().get(),
                meta.getState());
        return org.dbsyncer.sdk.util.TaskDetailUtil.assembleJoinedRow(detailRow, tgRow, metaRow);
    }

    /**
     * 分页拉取任务明细分表全部行（每页 {@link #DETAIL_FETCH_PAGE_SIZE}，循环累加）。
     *
     * @param taskId     任务 ID
     * @param detailType 明细 TYPE 过滤(可空)
     * @return 明细行列表
     */
    private List<Map<String, Object>> queryDetailRows(String taskId, String detailType) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Query query = new Query();
        query.setType(StorageEnum.TASK_DETAIL);
        query.setMetaId(taskId);
        query.setPageSize(DETAIL_FETCH_PAGE_SIZE);
        if (StringUtil.isNotBlank(detailType)) {
            query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, detailType);
        }
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            for (Object item : paging.getData()) {
                if (item instanceof Map) {
                    rows.add(new HashMap<>((Map<String, Object>) item));
                }
            }
            if (paging.getData().size() < DETAIL_FETCH_PAGE_SIZE) {
                break;
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return rows;
    }

    private Map<String, Map<String, Object>> buildTableGroupDisplayMap(String taskId) {
        List<TableGroup> groups = profileComponent.getTableGroupAll(taskId);
        Map<String, Map<String, Object>> map = new HashMap<>();
        if (CollectionUtils.isEmpty(groups)) {
            return map;
        }
        for (TableGroup group : groups) {
            Map<String, Object> row = new HashMap<>();
            row.put(ConfigConstant.TABLE_GROUP_SORT_INDEX, group.getIndex());
            row.put(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, group.getSourceDatabase());
            row.put(ConfigConstant.TABLE_GROUP_TARGET_DATABASE, group.getTargetDatabase());
            row.put(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, group.getSourceSchema());
            row.put(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA, group.getTargetSchema());
            row.put(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, group.getSourceTotal());
            row.put(ConfigConstant.TABLE_GROUP_TARGET_TOTAL, group.getTargetTotal());
            Table sourceTable = group.getSourceTable();
            Table targetTable = group.getTargetTable();
            if (sourceTable != null) {
                row.put(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, sourceTable.getName());
            }
            if (targetTable != null) {
                row.put(ConfigConstant.TABLE_GROUP_TARGET_TABLE, targetTable.getName());
            }
            if (StringUtil.isNotBlank(group.getId())) {
                map.put(group.getId(), row);
            }
        }
        return map;
    }
}
