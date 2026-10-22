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

    String SUBJECT_ROLE = "ROLE";
    String SUBJECT_USER = "USER";

    /** 业务分组（组内叶子继承） */
    String RESOURCE_GROUP = "group";
    /** 连接 */
    String RESOURCE_CONNECTOR = "connector";
    /** 同步任务 */
    String RESOURCE_MAPPING = "mapping";
    /** 订正校验任务 */
    String RESOURCE_VALIDATE_SYNC = "validateSync";
    /** 整库迁移任务 */
    String RESOURCE_DATABASE_SYNC = "databaseSync";

    String ACTION_READ = "read";
    String ACTION_WRITE = "write";
    String ACTION_OPERATE = "operate";

    List<ResourceAclVO> listAll();

    ResourceAclVO get(String id);

    /**
     * 新增或覆盖（同主体+资源唯一键则更新）。
     */
    String save(Map<String, String> params);

    String remove(Map<String, String> params);

    /**
     * 创建/复制后授予操作者完整叶子 ACL（read,write,operate）。
     * admin 或无效用户跳过；已存在则覆盖为完整动作。
     */
    void grantOwner(String username, String resourceType, String resourceId);

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
     * <ul>
     *   <li>{@code null}：不过滤（admin）</li>
     *   <li>空集：无任何权限</li>
     *   <li>非空：仅这些 ID</li>
     * </ul>
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
