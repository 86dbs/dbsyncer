/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.ResourceAcl;

import java.util.List;

/**
 * 资源 ACL 读写。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public interface ResourceAclProfile {

    ResourceAcl get(String id);

    List<ResourceAcl> listAll();

    List<ResourceAcl> listBySubject(int subjectType, String subjectId);

    ResourceAcl getByUnique(int subjectType, String subjectId, String resourceType, String resourceId);

    String add(ResourceAcl acl);

    String update(ResourceAcl acl);

    void remove(String id);

    /**
     * 按资源类型 + 资源 ID 批量删除（资源被删除时收尾）。
     */
    void removeByResource(String resourceType, String resourceId);

    /**
     * 按主体类型 + 主体 ID 批量删除（用户/角色被删除时收尾）。
     */
    void removeBySubject(int subjectType, String subjectId);
}
