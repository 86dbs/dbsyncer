/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ResourceAclVO;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 资源 ACL（授分组为主）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public interface ResourceAclService {

    List<ResourceAclVO> listAll();

    ResourceAclVO get(String id);

    /**
     * 新增或覆盖（同主体+资源唯一键则更新）。
     */
    String save(Map<String, String> params);

    String remove(Map<String, String> params);

    /**
     * 授予用户成为该资源的所有者。
     * admin 或无效用户跳过；已存在则覆盖为可读写。
     */
    void grantOwner(String username, String resourceType, String resourceId);

    /**
     * 资源被删除时收尾：清除指向该资源的全部 ACL。
     */
    void revokeAllForResource(String resourceType, String resourceId);

    /**
     * 用户/角色被删除时收尾：清除该主体的全部 ACL。
     *
     * @param subjectType 主体类型编码，见 {@link org.dbsyncer.biz.enums.ResourceAclSubjectEnum}
     */
    void revokeAllForSubject(int subjectType, String subjectId);

    /**
     * 用户对资源是否具备动作（admin 跳过）。
     */
    boolean check(String username, String resourceType, String resourceId, String action);

    /**
     * 断言具备资源动作，否则抛错。
     */
    void require(String username, String resourceType, String resourceId, String action);

    /**
     * 解析用户对某类资源可访问的 ID 集合。
     */
    Set<String> resolveAllowedIds(String username, String resourceType, String action);

    /**
     * 将 ACL 约束并入分组筛选结果（列表查询用）。
     */
    GroupSearchFilter applyToSearchFilter(GroupSearchFilter groupFilter, Set<String> allowedIds);

    /**
     * 过滤列表中无对应动作权限的项（admin 原样返回）。
     */
    <T> List<T> filter(String username, String resourceType, List<T> list, Function<T, String> idGetter, String action);
}
