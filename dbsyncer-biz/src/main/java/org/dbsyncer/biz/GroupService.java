/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.GroupRel;

import java.util.List;
import java.util.Map;

/**
 * 业务分组服务（资源与分组多对多）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public interface GroupService {

    /**
     * 新建分组（仅名称，SORT 默认 0）。
     *
     * @param params 含 name
     * @return 分组 ID
     */
    String add(Map<String, String> params);

    /**
     * 编辑分组（改名）。
     *
     * @param params 含 id、name
     * @return 分组 ID
     */
    String edit(Map<String, String> params);

    /**
     * 删除分组并级联清 rel。
     *
     * @param id 分组 ID
     * @return 分组 ID
     */
    String remove(String id);

    /**
     * 全部分组列表。
     */
    List<Group> list();

    /**
     * 按 id 查分组。
     */
    Group get(String id);

    /**
     * 分组下成员数（删组确认文案用）。
     */
    int countMembers(String id);

    /**
     * 挂到指定分组（幂等；已存在则忽略）。{@code groupId} 空表示清除该资源全部归属。
     *
     * @param groupId      目标分组 ID，空或空白=清除全部归属
     * @param resourceType connector / mapping / validateSync / databaseSync
     * @param resourceId   资源 ID
     */
    void bind(String groupId, String resourceType, String resourceId);

    /**
     * 批量挂组。{@code groupId} 空表示批量清除这些资源的全部归属。
     */
    void bindBatch(String groupId, String resourceType, List<String> resourceIds);

    /**
     * 从指定分组移出（不影响其它组归属）。
     */
    void unbind(String groupId, String resourceType, String resourceId);

    /**
     * 批量从指定分组移出。
     */
    void unbindBatch(String groupId, String resourceType, List<String> resourceIds);

    /**
     * 查询资源当前全部归属。
     */
    List<GroupRel> listByResource(String resourceType, String resourceId);

    /**
     * 按分组+类型取资源 ID 列表。
     */
    List<String> listResourceIds(String groupId, String resourceType);

    /**
     * 解析列表页 groupId 筛选项。
     *
     * @param groupIdParam 缺省/空/null=全部；其它=真实分组 ID
     * @param resourceType 资源类型
     */
    GroupSearchFilter resolveSearchFilter(String groupIdParam, String resourceType);
}
