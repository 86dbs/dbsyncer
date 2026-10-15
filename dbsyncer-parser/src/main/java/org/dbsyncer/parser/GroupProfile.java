/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Group;

import java.util.List;

/**
 * 业务分组读写。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public interface GroupProfile {

    /**
     * 按 id 查询分组。
     */
    Group getGroup(String id);

    /**
     * 全部分组（按名称升序）。
     */
    List<Group> listGroups();

    /**
     * 按名称精确查询（用于唯一性校验）。
     */
    Group getByName(String name);

    /**
     * 新增分组。
     */
    String addGroup(Group group);

    /**
     * 更新分组。
     */
    String updateGroup(Group group);

    /**
     * 删除分组（调用方负责级联清 rel，或由 Service 编排）。
     */
    void removeGroup(String id);

}
