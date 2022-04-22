package org.dbsyncer.listener.postgresql.column;

import org.postgresql.geometric.*;
import org.postgresql.util.PGmoney;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/22 22:39
 * @see org.postgresql.jdbc.TypeInfoCache
 */
public interface ColumnValue {

    boolean isNull();

    String asString();

    Boolean asBoolean();

    Integer asInteger();

    Long asLong();

    Float asFloat();

    Double asDouble();

    Object asDecimal();

    LocalDate asLocalDate();

    OffsetDateTime asOffsetDateTimeAtUtc();

    Instant asInstant();

    Object asTime();

    Object asLocalTime();

    OffsetTime asOffsetTimeUtc();

    byte[] asByteArray();

    PGbox asBox();

    PGcircle asCircle();

    Object asInterval();

    PGline asLine();

    Object asLseg();

    PGmoney asMoney();

    PGpath asPath();

    PGpoint asPoint();

    PGpolygon asPolygon();

    boolean isArray();

    Object asArray();

}