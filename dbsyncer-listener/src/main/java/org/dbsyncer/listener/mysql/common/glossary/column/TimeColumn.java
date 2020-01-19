package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class TimeColumn implements Column {
    private static final long serialVersionUID = 2408833111678694298L;

    private final java.sql.Time value;

    private TimeColumn(java.sql.Time value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public java.sql.Time getValue() {
        return this.value;
    }

    public static final TimeColumn valueOf(java.sql.Time value) {
        return new TimeColumn(value);
    }
}
