/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/27 23:14
 */
public interface TableGroupService {

    /**
     * 新增表关系（会提交表级统计任务）。
     */
    String add(Map<String, String> params);

    /**
     * 批量新增表关系，不提交统计任务（由调用方统一 MappingCountTask）。
     */
    int addWithoutCount(Map<String, String> params);

    /**
     * 按多组参数批量新增表关系（每组可含独立 fieldMappings）；遇重复跳过；不提交统计任务。
     *
     * @return 实际新增条数
     */
    int addWithoutCountBatch(List<Map<String, String>> paramsList);

    /**
     * 修改表关系
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 刷新表字段
     *
     * @param id
     */
    String refreshFields(String id);

    /**
     * 删除表关系
     *
     * @param mappingId
     * @param ids
     */
    String remove(String mappingId, String ids);

    /**
     * 获取表关系
     *
     * @param id
     * @return
     */
    TableGroup getTableGroup(String id);

    /**
     * 分页搜索
     *
     * @param params
     * @return
     */
    Paging<TableGroup> search(Map<String, String> params);

    /**
     * 重置任务 Meta：清空运行状态，并可选写入增量点；不汇总 total。
     */
    void resetMeta(Mapping mapping, String metaSnapshot);

    /**
     * 按表组 SOURCE_TOTAL 汇总并写回任务 Meta.total（全量）。
     */
    Meta refreshMetaTotal(Mapping mapping);
}
