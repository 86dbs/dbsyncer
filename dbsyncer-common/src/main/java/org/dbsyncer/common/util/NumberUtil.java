package org.dbsyncer.common.util;

import org.apache.commons.lang3.math.NumberUtils;

public abstract class NumberUtil {

    public static int toInt(String str) {
        return NumberUtils.toInt(str);
    }

    public static boolean isCreatable(String str) {
        return NumberUtils.isCreatable(str);
    }
}