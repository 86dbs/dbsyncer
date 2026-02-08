package org.dbsyncer.common.util;

import java.util.UUID;
import java.util.regex.Pattern;

public abstract class UUIDUtil {

    /**
     * 匹配格式：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (8-4-4-4-12)
     */
    private static final Pattern UUID_PATTERN_8_4_4_4_12 = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    public static String getUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public static boolean isUUID(String s) {
        return UUID_PATTERN_8_4_4_4_12.matcher(s).matches();
    }

    public static UUID fromString(String s) {
        return UUID.fromString(s);
    }
}
