/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.MappingVo;

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
    String add(Map<String, String> params) throws Exception;

    /**
     * 复制驱动
     *
     * @param id
     */
    String copy(String id) throws Exception;

    /**
     * 修改驱动
     *
     * @param params
     */
    String edit(Map<String, String> params) throws Exception;

    /**
     * 删除驱动
     *
     * @param id
     */
    String remove(String id) throws Exception;

    /**
     * 获取驱动
     *
     * @param id
     * @return
     */
    MappingVo getMapping(String id);

    /**
     * 获取驱动
     *
     * @param id
     * @param exclude 0-过滤已添加的表；1-显示所有表，包含已添加的表
     * @return
     */
    MappingVo getMapping(String id, Integer exclude) throws Exception;

    /**
     * 获取所有驱动
     *
     * @return
     */
    List<MappingVo> getMappingAll();

    /**
     * 启动驱动
     *
     * @param id
     */
    String start(String id) throws Exception;

    /**
     * 停止驱动
     *
     * @param id
     */
    String stop(String id) throws Exception;

    /**
     * 重置混合同步任务
     *
     * @param id
     */
    String reset(String id) throws Exception;

    /**
     * 刷新驱动数据源和目标源表
     *
     * @param id
     */
    String refreshMappingTables(String id) throws Exception;

}