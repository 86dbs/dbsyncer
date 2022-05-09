package org.dbsyncer.connector.file.column;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 22:39
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