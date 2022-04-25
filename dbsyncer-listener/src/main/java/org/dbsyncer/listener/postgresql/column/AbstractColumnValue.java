package org.dbsyncer.listener.postgresql.column;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.listener.ListenerException;
import org.postgresql.PGStatement;
import org.postgresql.geometric.*;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractColumnValue implements ColumnValue {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public LocalDate asLocalDate() {
        return DateFormatUtil.stringToLocalDate(asString());
    }

    @Override
    public Object asTime() {
        return asString();
    }

    @Override
    public Object asLocalTime() {
        return DateFormatUtil.stringToLocalTime(asString());
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        return DateFormatUtil.timeWithTimeZone(asString());
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if ("infinity".equals(asString())) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY), ZoneOffset.UTC);
        } else if ("-infinity".equals(asString())) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY), ZoneOffset.UTC);
        }
        return DateFormatUtil.timestampWithTimeZoneToOffsetDateTime(asString());
    }

    @Override
    public Instant asInstant() {
        if ("infinity".equals(asString())) {
            return toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY);
        } else if ("-infinity".equals(asString())) {
            return toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY);
        }
        return DateFormatUtil.timestampToInstant(asString());
    }

    @Override
    public PGbox asBox() {
        try {
            return new PGbox(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public PGcircle asCircle() {
        try {
            return new PGcircle(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse circle {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public Object asInterval() {
        try {
            return new PGInterval(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public PGline asLine() {
        try {
            return new PGline(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public PGlseg asLseg() {
        try {
            return new PGlseg(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
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

    @Override
    public PGpath asPath() {
        try {
            return new PGpath(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public PGpoint asPoint() {
        try {
            return new PGpoint(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public PGpolygon asPolygon() {
        try {
            return new PGpolygon(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new ListenerException(e);
        }
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public Object asArray() {
        return null;
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