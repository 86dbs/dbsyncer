package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.parser.model.TableGroup;

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
    boolean remove(String id);

    /**
     * 获取驱动
     *
     * @param id
     * @return
     */
    MappingVo getMapping(String id);

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
    boolean start(String id);

    /**
     * 停止驱动
     *
     * @param id
     */
    boolean stop(String id);

    /**
     * 获取驱动数据源和目标源表公共字段
     *
     * @return
     */
    TableGroup getTableGroup();

}