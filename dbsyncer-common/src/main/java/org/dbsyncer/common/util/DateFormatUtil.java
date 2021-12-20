package org.dbsyncer.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public abstract class DateFormatUtil {

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public static final DateTimeFormatter CHINESE_STANDARD_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /**
     * yyyy-MM-dd
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /**
     * HH:mm:ss
     */
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static ZoneId zoneId = ZoneId.systemDefault();

    public static String getCurrentTime() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }

    public static String dateToString(Date date) {
        return date.toInstant().atZone(zoneId).toLocalDate().format(DATE_FORMATTER);
    }

    public static String dateToChineseStandardTimeString(Date date) {
        return date.toInstant().atZone(zoneId).toLocalDateTime().format(CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static Date stringToDate(String s) {
        LocalDate localDate = LocalDate.parse(s, DATE_FORMATTER);
        Instant instant = localDate.atStartOfDay().atZone(zoneId).toInstant();
        return Date.from(instant);
    }

}