/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.column;

import org.dbsyncer.common.column.AbstractColumnValue;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.postgresql.PGStatement;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public final class PgColumnValue extends AbstractColumnValue<String> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public PgColumnValue(String value) {
        setValue(value);
    }

    @Override
    public String asString() {
        if (isEmpty(getValue())) {
            return null;
        }
        return getValue();
    }

    @Override
    public byte[] asByteArray() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        if (value.length() < 2) {
            return new byte[0];
        }
        return StringUtil.hexStringToByteArray(value.substring(2));
    }

    @Override
    public Byte asByte() {
        return 0;
    }

    @Override
    public Short asShort() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Short.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Short from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Integer asInteger() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Integer from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Long asLong() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Long from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Float asFloat() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Float.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Float from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Double asDouble() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse Double from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Boolean asBoolean() {
        String value = getValue();
        if (isEmpty(value)) {
            return false;
        }
        return "t".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse BigDecimal from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Date asDate() {
        String value = asString();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return DateFormatUtil.stringToDate(value);
        } catch (Exception e) {
            logger.warn("Failed to parse Date from value: {}", value, e);
            return null;
        }
    }

    @Override
    public Timestamp asTimestamp() {
        String value = asString();
        if ("infinity".equals(value)) {
            return Timestamp.from(toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY));
        } else if ("-infinity".equals(value)) {
            return Timestamp.from(toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY));
        } else if (isEmpty(value)) {
            return null;
        }
        return DateFormatUtil.timeWithoutTimeZoneToTimestamp(value);
    }

    @Override
    public Time asTime() {
        String value = getValue();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return Time.valueOf(value);
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to parse Time from value: {}", value, e);
            return null;
        }
    }

    public LocalTime asLocalTime() {
        String value = asString();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return DateFormatUtil.stringToLocalTime(value);
        } catch (Exception e) {
            logger.warn("Failed to parse LocalTime from value: {}", value, e);
            return null;
        }
    }

    public OffsetTime asOffsetTimeUtc() {
        String value = asString();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return DateFormatUtil.timeWithTimeZone(value);
        } catch (Exception e) {
            logger.warn("Failed to parse OffsetTime from value: {}", value, e);
            return null;
        }
    }

    public OffsetDateTime asOffsetDateTimeAtUtc() {
        String value = asString();
        if (isEmpty(value)) {
            return null;
        }
        if ("infinity".equals(value)) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY), ZoneOffset.UTC);
        } else if ("-infinity".equals(value)) {
            return OffsetDateTime.ofInstant(toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY), ZoneOffset.UTC);
        }
        try {
            return DateFormatUtil.timestampWithTimeZoneToOffsetDateTime(value);
        } catch (Exception e) {
            logger.warn("Failed to parse OffsetDateTime from value: {}", value, e);
            return null;
        }
    }

    public PGbox asBox() {
        try {
            return new PGbox(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
        }
    }

    public PGcircle asCircle() {
        try {
            return new PGcircle(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse circle {}, {}", asString(), e);
            throw new PostgreSQLException(e);
        }
    }

    public Object asInterval() {
        try {
            return new PGInterval(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
        }
    }

    public PGline asLine() {
        try {
            return new PGline(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
        }
    }

    public PGlseg asLseg() {
        try {
            return new PGlseg(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
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
            throw new PostgreSQLException(e);
        }
    }

    public PGpath asPath() {
        try {
            return new PGpath(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
        }
    }

    public PGpoint asPoint() {
        String value = asString();
        if (isEmpty(value)) {
            return null;
        }
        try {
            return new PGpoint(value);
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", value, e);
            throw new PostgreSQLException(e);
        }
    }

    public PGpolygon asPolygon() {
        try {
            return new PGpolygon(asString());
        } catch (final SQLException e) {
            logger.error("Failed to parse point {}, {}", asString(), e);
            throw new PostgreSQLException(e);
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