package org.dbsyncer.biz;

import org.dbsyncer.parser.model.ProjectGroup;

import java.util.List;
import java.util.Map;

/**
 * 项目组
 *
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
public interface ProjectGroupService {

    /**
     * 新增项目组
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 修改项目组
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 删除项目组
     *
     * @param id
     */
    String remove(String id);

    /**
     * 获取项目组
     *
     * @param id
     * @return
     */
    ProjectGroup getProjectGroup(String id);

    /**
     * 获取所有项目组
     *
     * @return
     */
    List<ProjectGroup> getProjectGroupAll();

    /**
     * 获取项目组详细
     *
     * @return
     */
    ProjectGroup getProjectGroupDetail(String projectGroupId);

}
