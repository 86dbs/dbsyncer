package org.dbsyncer.common.util;

import org.apache.commons.lang3.BooleanUtils;

public abstract class BooleanUtil {

    public static boolean toBoolean(final String str) {
        return BooleanUtils.toBoolean(str);
    }
}