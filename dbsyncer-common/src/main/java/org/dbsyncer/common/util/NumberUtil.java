package org.dbsyncer.common.util;

import org.apache.commons.lang3.math.NumberUtils;

public abstract class NumberUtil {

    public static int toInt(String str) {
        return NumberUtils.toInt(str);
    }

    public static int toInt(String str, int defaultValue) {
        return NumberUtils.toInt(str, defaultValue);
    }

    public static long toLong(String str) {
        return NumberUtils.toLong(str);
    }

    public static long toLong(String str, long defaultValue) {
        return NumberUtils.toLong(str, defaultValue);
    }

    public static long toLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return NumberUtil.toLong(String.valueOf(value), 0L);
    }

    public static boolean isCreatable(String str) {
        return NumberUtils.isCreatable(str);
    }
}
