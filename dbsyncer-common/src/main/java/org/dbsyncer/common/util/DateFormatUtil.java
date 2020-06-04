package org.dbsyncer.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public abstract class DateFormatUtil {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static ZoneId zoneId = ZoneId.systemDefault();

    public DateFormatUtil() {
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now().format(dateTimeFormatter);
    }

    public static String dateToString(Date date){
        String format = date.toInstant().atZone(zoneId).toLocalDate().format(dateFormatter);
        return format;
    }

    public static Date stringToDate(String s){
        LocalDate localDate = LocalDate.parse(s, dateFormatter);
        Instant instant = localDate.atStartOfDay().atZone(zoneId).toInstant();
        return Date.from(instant);
    }

}