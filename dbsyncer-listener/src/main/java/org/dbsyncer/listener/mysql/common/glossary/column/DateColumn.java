package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class DateColumn implements Column {
    private static final long serialVersionUID = 959710929844516680L;

    private final java.sql.Date value;

    private DateColumn(java.sql.Date value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public java.sql.Date getValue() {
        return this.value;
    }

    public static final DateColumn valueOf(java.sql.Date value) {
        return new DateColumn(value);
    }
}
