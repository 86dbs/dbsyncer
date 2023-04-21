package org.dbsyncer.listener.postgresql.column;

import org.dbsyncer.common.column.AbstractColumnValue;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.ListenerException;
import org.postgresql.PGStatement;
import org.postgresql.geometric.*;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.concurrent.TimeUnit;

public final class PgColumnValue extends AbstractColumnValue<String> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public PgColumnValue(String value) {
        setValue(value);
    }

    @Override
    public String asString() {
        return getValue();
    }

    @Override
    public byte[] asByteArray() {
        return StringUtil.hexStringToByteArray(getValue().substring(2));
    }

    @Override
    public Short asShort() {
        return Short.valueOf(getValue());
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(getValue());
    }

    @Override
    public Long asLong() {
        return Long.valueOf(getValue());
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(getValue());
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(getValue());
    }

    @Override
    public Boolean asBoolean() {
        return "t".equalsIgnoreCase(getValue());
    }

    @Override
    public BigDecimal asBigDecimal() {
        return new BigDecimal(getValue());
    }

    @Override
    public Date asDate() {
        return DateFormatUtil.stringToDate(asString());
    }

    @Override
    public Timestamp asTimestamp() {
        if ("infinity".equals(asString())) {
            return Timestamp.from(toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY));
        } else if ("-infinity".equals(asString())) {
            return Timestamp.from(toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY));
        }
        return DateFormatUtil.timeWithoutTimeZoneToTimestamp(asString());
    }

    @Override
    public Time asTime() {
        return Time.valueOf(getValue());
    }

    public LocalTime asLocalTime() {
        return DateFormatUtil.stringToLocalTime(asString());
    }

    public OffsetTime asOffsetTimeUtc() {
        return DateFormatUtil.timeWithTimeZone(asString());
    }

    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if ("infinity".equals(asString())) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY), ZoneOffset.UTC);
        } else if ("-infinity".equals(asString())) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY), ZoneOffset.UTC);
        }
        return DateFormatUtil.timestampWithTimeZoneToOffsetDateTime(asString());
    }

    public PGbox asBox() {
        try {
            return new PGbox(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGcircle asCircle() {
        try {
            return new PGcircle(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse circle {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public Object asInterval() {
        try {
            return new PGInterval(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGline asLine() {
        try {
            return new PGline(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGlseg asLseg() {
        try {
            return new PGlseg(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGmoney asMoney() {
        try {
            final String value = asString();
            if (value != null && value.startsWith("-")) {
                final String negativeMoney = "(" + value.substring(1) + ")";
                return new PGmoney(negativeMoney);
            }
            return new PGmoney(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse money {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGpath asPath() {
        try {
            return new PGpath(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGpoint asPoint() {
        try {
            return new PGpoint(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    public PGpolygon asPolygon() {
        try {
            return new PGpolygon(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    private Instant toInstantFromMicros(long microsSinceEpoch) {
        return Instant.ofEpochSecond(
                TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
                TimeUnit.MICROSECONDS.toNanos(microsSinceEpoch % TimeUnit.SECONDS.toMicros(1)));
    }

    private Instant toInstantFromMillis(long millisecondSinceEpoch) {
        return Instant.ofEpochSecond(
                TimeUnit.MILLISECONDS.toSeconds(millisecondSinceEpoch),
                TimeUnit.MILLISECONDS.toNanos(millisecondSinceEpoch % TimeUnit.SECONDS.toMillis(1)));
    }

}