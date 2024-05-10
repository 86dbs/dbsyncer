/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;
import org.dbsyncer.common.column.Lexer;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public abstract class DateFormatUtil {

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public static final DateTimeFormatter CHINESE_STANDARD_TIME_FORMATTER = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd HH:mm:ss");
    /**
     * yyyy-MM-dd'T'HH:mm:ss.SSSZ
     */
    public static final DateTimeFormatter TS_TZ_WITH_MILLISECOND_FORMATTER = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    /**
     * yyyy-MM-dd'T'HH:mm:ssZ
     */
    public static final DateTimeFormatter TS_TZ_FORMATTER = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd'T'HH:mm:ssZ");
    /**
     * yyyy-MM-dd
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd");
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

    private static final DateTimeFormatter TS_FORMAT = new DateTimeFormatterBuilder()
            .append(NON_ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalStart()
            .appendLiteral(" ")
            .appendText(ChronoField.ERA, TextStyle.SHORT)
            .optionalEnd()
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

    private static final DateTimeFormatter TS_TZ_FORMAT_ORACLE = new DateTimeFormatterBuilder()
            .append(NON_ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendLiteral(' ')
            .appendOffset("+HH:mm", "")
            .toFormatter();

    public static String getCurrentTime() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }

    public static String dateToString(Date date) {
        return date.toLocalDate().format(DATE_FORMATTER);
    }

    public static String dateToString(java.util.Date date) {
        return date.toInstant().atZone(zoneId).toLocalDateTime()
                .format(CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static Date stringToDate(String s) {
        return Date.valueOf(LocalDate.parse(s, DATE_FORMATTER));
    }

    public static Date stringToDate(String s, DateTimeFormatter formatter) {
        return Date.valueOf(LocalDate.parse(s, formatter));
    }

    public static String timestampToString(Timestamp timestamp) {
        return timestamp.toLocalDateTime().format(CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static LocalTime stringToLocalTime(String s) {
        return LocalTime.parse(s, CHINESE_STANDARD_TIME_FORMATTER);
    }

    public static Timestamp stringToTimestamp(String s) {
        try {
            // 2020-7-12 00:00:00
            if (s.length() < 19) {
                return Timestamp.valueOf(
                        LocalDateTime.from(CHINESE_STANDARD_TIME_FORMATTER.parse(format(s))));
            }

            // 2020-07-12 00:00:00
            if (s.length() == 19) {
                return Timestamp.valueOf(
                        LocalDateTime.from(CHINESE_STANDARD_TIME_FORMATTER.parse(s)));
            }

            // 2020-07-12 00:00:00.0
            if (s.length() == 21) {
                s = s.substring(0, s.lastIndexOf("."));
                return Timestamp.valueOf(
                        LocalDateTime.from(CHINESE_STANDARD_TIME_FORMATTER.parse(s)));
            }

            // 2022-07-21T05:00:59+0800
            if (s.length() == 24) {
                return stringToTimestamp(s, TS_TZ_FORMATTER);
            }

            // 2022-07-21T05:35:34.000+0800
            if (s.length() == 28) {
                return stringToTimestamp(s, TS_TZ_WITH_MILLISECOND_FORMATTER);
            }

            // 2022-07-21T05:35:34.000+08:00
            if (s.length() == 29) {
                s = s.replaceAll(":[^:]*$", "00");
                return stringToTimestamp(s, TS_TZ_WITH_MILLISECOND_FORMATTER);
            }

            throw new CommonException(String.format("Can not parse val[%s] to Timestamp", s));
        } catch (ParseException e) {
            throw new CommonException(e);
        }
    }

    public static Timestamp stringToTimestamp(String s, DateTimeFormatter formatter)
            throws ParseException {
        return Timestamp.valueOf(LocalDateTime.from(formatter.parse(s)).atZone(zoneId).toLocalDateTime());
    }

    public static Timestamp timeWithoutTimeZoneToTimestamp(String s) {
        return Timestamp.valueOf(
                LocalDateTime.from(DateFormatUtil.TS_FORMAT.parse(s)).atZone(ZoneOffset.UTC)
                        .toLocalDateTime());
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

    public static OffsetDateTime timestampWithTimeZoneToOffsetDateTimeOracle(String s) {
        return OffsetDateTime.from(TS_TZ_FORMAT_ORACLE.parse(s)).withOffsetSameInstant(ZoneOffset.UTC);
    }

    private static String format(String s) {
        StringBuilder buf = new StringBuilder();
        Lexer lexer = new Lexer(s);
        char comma = '-';
        // 年
        nextToken(lexer, buf, comma);
        // 月
        nextToken(lexer, buf, comma);
        // 日
        comma = ' ';
        nextToken(lexer, buf, comma);
        // 时
        comma = ':';
        nextToken(lexer, buf, comma);
        // 分
        nextToken(lexer, buf, comma);
        // 秒
        nextToken(lexer, buf, comma, false);
        return buf.toString();
    }

    private static void nextToken(Lexer lexer, StringBuilder buf, char comma) {
        nextToken(lexer, buf, comma, true);
    }

    private static void nextToken(Lexer lexer, StringBuilder buf, char comma, boolean appendComma) {
        buf.append(fillZero(lexer.nextToken(comma)));
        if (appendComma) {
            buf.append(comma);
        }
    }

    private static String fillZero(String s) {
        if (s.length() < 2) {
            return String.format("%02d", Integer.parseInt(s));
        }
        return s;
    }

}