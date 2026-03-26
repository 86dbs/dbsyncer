/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;
import org.dbsyncer.common.column.Lexer;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
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

    // 常见日期时间格式模式
    public static final String PATTERN_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String PATTERN_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String PATTERN_YYYY_MM_DD_HH_MM_SS_SSSSSS = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String PATTERN_YYYY_MM_DD_T_HH_MM_SS_XXX = "yyyy-MM-dd'T'HH:mm:ssXXX";
    public static final String PATTERN_YYYY_MM_DD_T_HH_MM_SS_SSS_XXX = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    public static final String PATTERN_YYYY_MM_DD_T_HH_MM_SS_Z = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final String PATTERN_YYYY_MM_DD_T_HH_MM_SS_SSS_Z = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String PATTERN_YYYYMMDD = "yyyyMMdd";
    public static final String PATTERN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public static final String PATTERN_YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS";

    public static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD);
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_HH_MM_SS);
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_SSS = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_HH_MM_SS_SSS);
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_SSSSSS = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_HH_MM_SS_SSSSSS);
    /* 带时区格式 */
    public static final DateTimeFormatter YYYY_MM_DD_T_HH_MM_SS_XXX = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_T_HH_MM_SS_XXX);
    public static final DateTimeFormatter YYYY_MM_DD_T_HH_MM_SS_SSS_XXX = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_T_HH_MM_SS_SSS_XXX);
    public static final DateTimeFormatter YYYY_MM_DD_T_HH_MM_SS_Z = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_T_HH_MM_SS_Z);
    public static final DateTimeFormatter YYYY_MM_DD_T_HH_MM_SS_SSS_Z = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_T_HH_MM_SS_SSS_Z);

    // 兼容格式
    private static final DateTimeFormatter[] FORMATTERS = {YYYY_MM_DD, YYYY_MM_DD_HH_MM_SS, YYYY_MM_DD_HH_MM_SS_SSS, YYYY_MM_DD_HH_MM_SS_SSSSSS, YYYY_MM_DD_T_HH_MM_SS_XXX,
            YYYY_MM_DD_T_HH_MM_SS_SSS_XXX, YYYY_MM_DD_T_HH_MM_SS_Z, YYYY_MM_DD_T_HH_MM_SS_SSS_Z, DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.ISO_INSTANT};

    // 定义常见的日期时间格式模式
    private static final String[] SIMPLE_PATTERNS = {PATTERN_YYYY_MM_DD, PATTERN_YYYY_MM_DD_HH_MM_SS, PATTERN_YYYY_MM_DD_HH_MM_SS_SSS, PATTERN_YYYY_MM_DD_HH_MM_SS_SSSSSS, PATTERN_YYYYMMDD,
            PATTERN_YYYYMMDDHHMMSS, PATTERN_YYYYMMDDHHMMSSSSS};

    public static final DateTimeFormatter MM_DD = DateTimeFormatter.ofPattern("MM-dd");
    public static final DateTimeFormatter HH_MM_SS = DateTimeFormatter.ofPattern("HH:mm:ss");
    /**
     * 默认时区
     */
    private static final ZoneId zoneId = ZoneId.systemDefault();

    private static final DateTimeFormatter TIME_TZ_FORMAT = new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_TIME).appendOffset("+HH:mm", "").toFormatter();

    private static final DateTimeFormatter NON_ISO_LOCAL_DATE = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR_OF_ERA, 4, 10, SignStyle.NEVER).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2).toFormatter();

    private static final DateTimeFormatter TS_FORMAT = new DateTimeFormatterBuilder().append(NON_ISO_LOCAL_DATE).appendLiteral(' ').append(DateTimeFormatter.ISO_LOCAL_TIME).optionalStart()
            .appendLiteral(" ").appendText(ChronoField.ERA, TextStyle.SHORT).optionalEnd().toFormatter();
    private static final DateTimeFormatter TS_TZ_FORMAT = new DateTimeFormatterBuilder().append(NON_ISO_LOCAL_DATE).appendLiteral(' ').append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "").optionalStart().appendLiteral(" ").appendText(ChronoField.ERA, TextStyle.SHORT).optionalEnd().toFormatter();
    private static final DateTimeFormatter TS_TZ_WITH_SECONDS_FORMAT = new DateTimeFormatterBuilder().append(NON_ISO_LOCAL_DATE).appendLiteral(' ').append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOffset("+HH:MM:SS", "").optionalStart().appendLiteral(" ").appendText(ChronoField.ERA, TextStyle.SHORT).optionalEnd().toFormatter();

    private static final DateTimeFormatter TS_TZ_FORMAT_ORACLE = new DateTimeFormatterBuilder().append(NON_ISO_LOCAL_DATE).appendLiteral(' ').append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendLiteral(' ').appendOffset("+HH:mm", "").toFormatter();

    public static String getCurrentTime() {
        return LocalDateTime.now().format(HH_MM_SS);
    }

    public static String dateToString(Date date) {
        return date.toLocalDate().format(YYYY_MM_DD);
    }

    public static String dateToString(java.util.Date date) {
        return date.toInstant().atZone(zoneId).toLocalDateTime().format(YYYY_MM_DD_HH_MM_SS);
    }

    public static Date stringToDate(String s) {
        return Date.valueOf(LocalDate.parse(s, YYYY_MM_DD));
    }

    public static Date stringToDate(String s, DateTimeFormatter formatter) {
        return Date.valueOf(LocalDate.parse(s, formatter));
    }

    public static String timestampToString(Timestamp timestamp) {
        return timestampToString(timestamp, YYYY_MM_DD_HH_MM_SS);
    }

    public static String timestampToString(Timestamp timestamp, DateTimeFormatter formatter) {
        return timestamp.toLocalDateTime().format(formatter);
    }

    public static LocalTime stringToLocalTime(String s) {
        return LocalTime.parse(s, YYYY_MM_DD_HH_MM_SS);
    }

    public static Timestamp stringToTimestamp(String s) {
        if (StringUtil.isBlank(s)) {
            return null;
        }

        // 2024-06-05 21:15:13.
        if (StringUtil.endsWith(s, StringUtil.POINT)) {
            s = s.substring(0, s.lastIndexOf(StringUtil.POINT));
        }

        // 2020-7-12 00:00:00
        if (s.length() < 19) {
            return Timestamp.valueOf(LocalDateTime.from(YYYY_MM_DD_HH_MM_SS.parse(format(s))));
        }

        // 带时区的 ISO 字符串（如 2024-01-10T01:15:00.000Z、2024-01-10T09:15:00+08:00、2024-01-10T01:15:00.000-08:00）
        // 按 UTC 解析为 Instant，避免被 LocalDateTime.parse 当成本地时间再 Timestamp.valueOf 产生时区偏差
        if (s.contains("T") && (s.endsWith("Z") || s.contains("+") || s.matches(".*-\\d{2}:\\d{2}$"))) {
            try {
                return Timestamp.from(OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant());
            } catch (DateTimeParseException e) {
                try {
                    return Timestamp.from(Instant.parse(s));
                } catch (DateTimeParseException e2) {
                    // fall through to FORMATTERS loop
                }
            }
        }

        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                if (s.contains("T") && s.contains("+")) {
                    // 处理带时区的ISO格式
                    Instant instant = Instant.from(formatter.parse(s));
                    return Timestamp.from(instant);
                }
                // 处理本地时间格式
                return Timestamp.valueOf(LocalDateTime.parse(s, formatter));
            } catch (DateTimeParseException e) {
                // 继续尝试下一个格式
                continue;
            }
        }
        // 尝试SimpleDateFormat
        return parseWithSimpleDateFormat(s);
    }

    private static Timestamp parseWithSimpleDateFormat(String s) {
        for (String pattern : SIMPLE_PATTERNS) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                sdf.setLenient(false); // 严格模式
                return new Timestamp(sdf.parse(s).getTime());
            } catch (ParseException e) {
                // 继续尝试下一个格式
                continue;
            }
        }

        throw new CommonException(String.format("Can not parse val[%s] to Timestamp", s));
    }

    public static Timestamp stringToTimestamp(String s, DateTimeFormatter formatter) throws ParseException {
        return Timestamp.valueOf(LocalDateTime.from(formatter.parse(s)).atZone(zoneId).toLocalDateTime());
    }

    public static Timestamp timeWithoutTimeZoneToTimestamp(String s) {
        return Timestamp.valueOf(LocalDateTime.from(DateFormatUtil.TS_FORMAT.parse(s)).atZone(ZoneOffset.UTC).toLocalDateTime());
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

    // public static void main(String[] args) {
    // String[] dateTimeExamples = {
    // "2020-7-2 16:13:14",
    // "2020-07-12 16:13:14",
    // "2020-07-12 16:13:14.0",
    // "2020-07-12 16:13:14.016000",
    // "2022-07-21T05:00:59+0800",
    // "2022-07-21T05:00:59.111Z",
    // "2022-07-21T05:35:34.111+0800",
    // "2022-07-21T05:35:34.111+08:00"
    // };
    //
    // for (String example : dateTimeExamples) {
    // Timestamp timestamp = DateFormatUtil.stringToTimestamp(example);
    // if (timestamp != null) {
    // System.out.println(String.format("解析 '%s' -> %s", example, timestamp));
    // } else {
    // System.out.println("无法解析: " + example);
    // }
    // }
    // }

}