package org.dbsyncer.common.column;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/22 22:39
 */
public interface ColumnValue {

    boolean isNull();

    String asString();

    byte[] asByteArray();

    Short asShort();

    Integer asInteger();

    Long asLong();

    Float asFloat();

    Double asDouble();

    Boolean asBoolean();

    BigDecimal asDecimal();

    Date asDate();

    Timestamp asTimestamp();

    Time asTime();
}