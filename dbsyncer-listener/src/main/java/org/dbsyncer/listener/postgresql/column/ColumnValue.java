package org.dbsyncer.listener.postgresql.column;

import org.postgresql.geometric.*;
import org.postgresql.util.PGmoney;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/22 22:39
 * @see org.postgresql.jdbc.TypeInfoCache
 */
public interface ColumnValue {

    void setValue(String value);

    boolean isNull();

    String asString();

    Boolean asBoolean();

    Integer asInteger();

    Long asLong();

    Float asFloat();

    Double asDouble();

    Object asDecimal();

    Date asDate();

    OffsetDateTime asOffsetDateTimeAtUtc();

    Timestamp asTimestamp();

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