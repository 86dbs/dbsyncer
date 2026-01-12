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
     * 新增表关系
     *
     * @param params
     */
    String add(Map<String, String> params);

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
    boolean remove(String mappingId, String ids);

    /**
     * 获取表关系
     *
     * @param id
     * @return
     */
    TableGroup getTableGroup(String id);

    /**
     * 获取所有表关系
     *
     * @param mappingId
     * @return
     */
    List<TableGroup> getTableGroupAll(String mappingId);

    /**
     * 分页搜索
     *
     * @param params
     * @return
     */
    Paging<TableGroup> search(Map<String, String> params);

    /**
     * 更新元信息
     *
     * @param mapping
     * @param metaSnapshot
     */
    Meta updateMeta(Mapping mapping, String metaSnapshot);

}