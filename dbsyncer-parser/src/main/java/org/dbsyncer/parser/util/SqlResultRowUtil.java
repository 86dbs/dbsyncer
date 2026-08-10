/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SqlQuery 结果行别名规范化：忽略驱动返回的列名大小写，再按固定驼峰别名写出。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-10
 */
public final class SqlResultRowUtil {

    private SqlResultRowUtil() {
    }

    /**
     * 将 SQL 结果行按别名列表投影为稳定驼峰字段（避免 JSON 出现 TYPE/UPDATETIME）。
     *
     * @param sqlRow  驱动返回行，可为 null
     * @param aliases 目标别名（顺序保留）
     * @return 仅含命中别名的新 Map
     */
    public static Map<String, Object> toAliasRow(Map<String, Object> sqlRow, String[] aliases) {
        Map<String, Object> src = new LinkedCaseInsensitiveMap<>();
        if (sqlRow != null) {
            src.putAll(sqlRow);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        if (aliases == null) {
            return row;
        }
        for (String alias : aliases) {
            if (alias != null && src.containsKey(alias)) {
                row.put(alias, src.get(alias));
            }
        }
        return row;
    }
}
