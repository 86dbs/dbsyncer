/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.common.column;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-22 22:39
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

    BigDecimal asBigDecimal();

    Date asDate();

    Timestamp asTimestamp();

    Time asTime();
}