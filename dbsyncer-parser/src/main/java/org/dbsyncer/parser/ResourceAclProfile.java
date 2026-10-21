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

    List<ResourceAcl> listBySubject(String subjectType, String subjectId);

    List<ResourceAcl> listByResource(String resourceType, String resourceId);

    ResourceAcl getByUnique(String subjectType, String subjectId, String resourceType, String resourceId);

    String add(ResourceAcl acl);

    String update(ResourceAcl acl);

    void remove(String id);
}
