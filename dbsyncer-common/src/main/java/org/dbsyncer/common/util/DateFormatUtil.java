package org.dbsyncer.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public abstract class DateFormatUtil {

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static ZoneId zoneId = ZoneId.systemDefault();

    public static String getCurrentTime() {
        return LocalDateTime.now().format(timeFormatter);
    }

    public static String dateToString(Date date){
        return date.toInstant().atZone(zoneId).toLocalDate().format(dateFormatter);
    }

    public static Date stringToDate(String s){
        LocalDate localDate = LocalDate.parse(s, dateFormatter);
        Instant instant = localDate.atStartOfDay().atZone(zoneId).toInstant();
        return Date.from(instant);
    }

}