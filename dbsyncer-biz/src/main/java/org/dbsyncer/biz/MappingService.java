/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.MappingCustomTableVO;
import org.dbsyncer.biz.vo.MappingVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.sdk.model.Table;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:14
 */
public interface MappingService {

    /**
     * 新增驱动
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 复制驱动
     *
     * @param id
     */
    String copy(String id);

    /**
     * 修改驱动
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 删除驱动
     *
     * @param id
     */
    String remove(String id);

    /**
     * 获取驱动（不含全量表列表，表下拉请走 {@link #searchTables}）
     *
     * @param id
     * @return
     */
    MappingVO getMapping(String id);

    /**
     * 获取驱动自定义表信息
     *
     * @param id
     * @param type
     * @return
     */
    MappingCustomTableVO getMappingCustomTable(String id, String type);

    /**
     * 分页搜索
     *
     * @param params
     * @return
     */
    Paging<MappingVO> search(Map<String, String> params);

    /**
     * 同步任务详情：按表映射汇总成功/失败数。
     *
     * @param params id=mappingId, detailStatus=fail|success|空, pageNum, pageSize
     * @return 分页表映射汇总
     */
    Paging searchTableGroupResult(Map<String, String> params);

    /**
     * 启动驱动
     *
     * @param id
     */
    String start(String id);

    /**
     * 停止驱动
     *
     * @param id
     */
    String stop(String id);

    /**
     * 刷新驱动数据源和目标源表
     *
     * @param id
     */
    String refreshMappingTables(String id);

    /**
     * 分页搜索驱动表（用于下拉框远程搜索）
     *
     * @param params
     * @return
     */
    Paging<Table> searchTables(Map<String, String> params);

    /**
     * 查询自定义表
     *
     * @param params
     * @return
     */
    List<Table> searchCustomTable(Map<String, String> params);

    /**
     * 保存自定义表
     *
     * @param params
     * @return
     */
    String saveCustomTable(Map<String, String> params);

    /**
     * 删除自定义表
     *
     * @param params
     * @return
     */
    String removeCustomTable(Map<String, String> params);
}
