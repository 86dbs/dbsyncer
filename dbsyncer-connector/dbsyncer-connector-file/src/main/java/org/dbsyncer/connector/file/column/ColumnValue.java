/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.column;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
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

    Date asDate();

    Timestamp asTimestamp();

    Object asTime();

    byte[] asByteArray();

}