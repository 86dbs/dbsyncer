/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.GroupRel;

import java.util.List;

/**
 * 业务分组关系读写（多对多：一资源可挂多组）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public interface GroupRelProfile {

    /**
     * 按 (分组, 资源类型, 资源ID) 查一条归属。
     */
    GroupRel getByGroupAndResource(String groupId, String resourceType, String resourceId);

    /**
     * 按 (资源类型, 资源ID) 列出全部归属。
     */
    List<GroupRel> listByResource(String resourceType, String resourceId);

    /**
     * 按分组列出关系；{@code resourceType} 为空则不限类型。
     */
    List<GroupRel> listByGroup(String groupId, String resourceType);

    /**
     * 按分组 + 资源类型列出 RESOURCE_ID。
     */
    List<String> listResourceIds(String groupId, String resourceType);

    /**
     * 统计分组下成员数。
     */
    int countByGroup(String groupId);

    /**
     * 新增关系。
     */
    String add(GroupRel rel);

    /**
     * 更新关系。
     */
    String update(GroupRel rel);

    /**
     * 按主键删除。
     */
    void remove(String id);

    /**
     * 删除某资源在指定分组下的关系。
     */
    void removeByGroupAndResource(String groupId, String resourceType, String resourceId);

    /**
     * 删除某资源全部归属关系。
     */
    void removeByResource(String resourceType, String resourceId);

    /**
     * 删除某分组下全部关系。
     */
    void removeByGroup(String groupId);
}
