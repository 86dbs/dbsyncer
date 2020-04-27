package org.dbsyncer.common.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class DateFormatUtil {
    public DateFormatUtil() {
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}