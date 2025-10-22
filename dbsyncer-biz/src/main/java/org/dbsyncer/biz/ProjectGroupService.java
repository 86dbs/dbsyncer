/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.parser.model.ProjectGroup;

import java.util.List;
import java.util.Map;

/**
 * 分组管理服务
 *
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
public interface ProjectGroupService {

    /**
     * 新增分组
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 修改分组
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 删除分组
     *
     * @param id
     */
    String remove(String id);

    /**
     * 获取分组
     *
     * @param id
     * @return
     */
    ProjectGroupVo getProjectGroup(String id);

    /**
     * 获取未使用的分组
     * @return
     */
    ProjectGroupVo getProjectGroupUnUsed();

    /**
     * 获取所有分组
     *
     * @return
     */
    List<ProjectGroup> getProjectGroupAll();

}
