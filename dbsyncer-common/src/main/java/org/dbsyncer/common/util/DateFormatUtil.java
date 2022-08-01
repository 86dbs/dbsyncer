package org.dbsyncer.common.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public abstract class DateFormatUtil {

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public static final DateTimeFormatter CHINESE_STANDARD_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /**
     * yyyy-MM-dd'T'HH:mm:ss.SSSz
     */
    public static final DateFormat GMT_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");
    /**
     * yyyy-MM-dd
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /**
     * HH:mm:ss
     */
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    /**
     * 默认时区
     */
    private static final ZoneId zoneId = ZoneId.systemDefault();

    private static final DateTimeFormatter TIME_TZ_FORMAT = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "")
            .toFormatter();

    private static final DateTimeFormatter NON_ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 10, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter();

    private static final DateTimeFormatter TS_TZ_FORMAT = new DateTimeFormatterBuilder()
            .append(NON_ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "")
            .optionalStart()
            .appendLiteral(" ")
            .appendText(ChronoField.ERA, TextStyle.SHORT)
            .optionalEnd()
            .toFormatter();
    private static final DateTimeFormatter TS_TZ_WITH_SECONDS_FORMAT = new DateTimeFormatterBuilder()
            .append(NON_ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH:MM:SS", "")
            .optionalStart()
            .appendLiteral(" ")
            .appendText(ChronoField.ERA, TextStyle.SHORT)
            .optionalEnd()
            .toFormatter();

    public static String getCurrentTime() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }

    public static String dateToString(Date date) {
        return date.toLocalDate().format(DATE_FORMATTER);
    }

    public static String dateToString(java.util.Date date) {
        return date.toInstant().atZone(zoneId).toLocalDateTime().format(CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static Date stringToDate(String s) {
        return Date.valueOf(LocalDate.parse(s, DATE_FORMATTER));
    }

    public static String timestampToString(Timestamp timestamp) {
        return timestamp.toLocalDateTime().format(CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static LocalTime stringToLocalTime(String s) {
        return LocalTime.parse(s, CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static Timestamp stringToTimestamp(String s) {
        return Timestamp.valueOf(LocalDateTime.from(CHINESE_STANDARD_TIME_FORMATTER.parse(s)));
    }

    public static Timestamp stringToTimestamp(String s, DateFormat formatter) throws ParseException {
        return new Timestamp(formatter.parse(s).getTime());
    }

    public static OffsetTime timeWithTimeZone(String s) {
        return OffsetTime.parse(s, TIME_TZ_FORMAT).withOffsetSameInstant(ZoneOffset.UTC);
    }

    public static OffsetDateTime timestampWithTimeZoneToOffsetDateTime(String s) {
        TemporalAccessor parsedTimestamp;
        try {
            parsedTimestamp = TS_TZ_FORMAT.parse(s);
        } catch (DateTimeParseException e) {
            parsedTimestamp = TS_TZ_WITH_SECONDS_FORMAT.parse(s);
        }
        return OffsetDateTime.from(parsedTimestamp).withOffsetSameInstant(ZoneOffset.UTC);
    }

}