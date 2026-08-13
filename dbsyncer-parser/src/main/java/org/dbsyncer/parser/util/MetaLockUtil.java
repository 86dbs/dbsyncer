/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.StringUtil;

/**
 * Meta 写锁：全量并发时 {@code incrementMeta} 与 snapshot flush 共用，避免整行回写覆盖原子计数。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-11
 */
public abstract class MetaLockUtil {

    private MetaLockUtil() {
    }

    /**
     * 按 metaId 获取进程内互斥锁（{@link String#intern()}）。
     *
     * @param metaId Meta 主键
     * @return 锁对象；metaId 为空时返回专用占位锁
     */
    public static Object lock(String metaId) {
        String id = StringUtil.isBlank(metaId) ? StringUtil.EMPTY : metaId;
        return ("meta-write-" + id).intern();
    }
}
