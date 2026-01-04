/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.MappingCustomTableVO;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.common.model.Paging;

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
     * 获取驱动
     *
     * @param id
     * @return
     */
    MappingVo getMapping(String id);

    /**
     * 获取驱动自定义表信息
     *
     * @param id
     * @param type
     * @return
     */
    MappingCustomTableVO getMappingCustomTable(String id, String type);

    /**
     * 获取驱动
     *
     * @param id
     * @param exclude 0-过滤已添加的表；1-显示所有表，包含已添加的表
     * @return
     */
    MappingVo getMapping(String id, Integer exclude);

    /**
     * 获取所有驱动
     *
     * @return
     */
    List<MappingVo> getMappingAll();

    /**
     * 分页搜索
     *
     * @param params
     * @return
     */
    Paging<MappingVo> search(Map<String, String> params);

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

}