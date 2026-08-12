/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.StringUtil;

/**
 * 增量管道数校验与位运算路由。
 * <p>管道数须为 2 的幂，路由使用 {@code hashCode & (n - 1)}，避免取模。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-12
 */
public abstract class ChannelSizeUtil {

    /**
     * 默认管道数
     */
    public static final int DEFAULT_SIZE = 8;

    /**
     * 管道数上限
     */
    public static final int MAX_SIZE = 64;

    private ChannelSizeUtil() {
    }

    /**
     * 是否为合法管道数（1~64 且为 2 的幂）。
     *
     * @param channelSize 管道数
     * @return 合法返回 true
     */
    public static boolean isValid(int channelSize) {
        return channelSize >= 1 && channelSize <= MAX_SIZE && (channelSize & (channelSize - 1)) == 0;
    }

    /**
     * 非法值回落到默认 8。
     *
     * @param channelSize 管道数
     * @return 合法管道数
     */
    public static int normalize(int channelSize) {
        return isValid(channelSize) ? channelSize : DEFAULT_SIZE;
    }

    /**
     * 按表名位运算得到管道下标。
     *
     * @param tableName   源表名
     * @param channelSize 管道数（非法时按默认 8）
     * @return [0, n)
     */
    public static int resolveChannelIndex(String tableName, int channelSize) {
        int size = normalize(channelSize);
        if (StringUtil.isBlank(tableName)) {
            return 0;
        }
        return tableName.hashCode() & (size - 1);
    }
}
