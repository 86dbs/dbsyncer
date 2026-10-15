/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.util.Collections;
import java.util.List;

/**
 * 按分组筛选资源列表的解析结果（仅支持 ID IN / 不过滤 / 空结果）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public class GroupSearchFilter {

    /**
     * 不过滤（全部）
     */
    public static final GroupSearchFilter NONE = new GroupSearchFilter(false, Collections.emptyList());

    /**
     * 直接返回空分页（组内无成员）
     */
    public static final GroupSearchFilter EMPTY = new GroupSearchFilter(true, Collections.emptyList());

    private final boolean emptyResult;
    private final List<String> ids;

    public GroupSearchFilter(boolean emptyResult, List<String> ids) {
        this.emptyResult = emptyResult;
        this.ids = ids == null ? Collections.emptyList() : ids;
    }

    public boolean isEmptyResult() {
        return emptyResult;
    }

    public List<String> getIds() {
        return ids;
    }

    public boolean hasIdFilter() {
        return !emptyResult && !ids.isEmpty();
    }
}
