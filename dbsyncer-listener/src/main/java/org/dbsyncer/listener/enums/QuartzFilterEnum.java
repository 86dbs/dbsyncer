package org.dbsyncer.listener.enums;

import org.dbsyncer.listener.QuartzFilter;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/05/30 14:19
 */
public enum QuartzFilterEnum {

    /**
     * 时间戳
     */
    TIME_STAMP("$timestamp$", "系统时间戳", new QuartzFilter() {
        @Override
        public Object getObject() {
            return new Timestamp(Instant.now().toEpochMilli());
        }

        @Override
        public Object getObject(String s) {
            return new Timestamp(Long.parseLong(s));
        }

        @Override
        public String toString(Object value) {
            Timestamp ts = (Timestamp) value;
            return String.valueOf(ts.getTime());
        }
    }),
    /**
     * 日期
     */
    DATE("$date$", "系统日期", new QuartzFilter() {
        @Override
        public Object getObject() {
            return new Date();
        }

        @Override
        public Object getObject(String s) {
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
            return null;
        }

        @Override
        public String toString(Object value) {
            return String.valueOf(value);
        }
    });

    private String type;
    private String message;
    private QuartzFilter quartzFilter;

    QuartzFilterEnum(String type, String message, QuartzFilter quartzFilter) {
        this.type = type;
        this.message = message;
        this.quartzFilter = quartzFilter;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public QuartzFilter getQuartzFilter() {
        return quartzFilter;
    }
}