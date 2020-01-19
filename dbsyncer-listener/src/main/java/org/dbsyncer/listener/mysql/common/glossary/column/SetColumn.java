package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class SetColumn implements Column {
    private static final long serialVersionUID = -5274295462701023264L;

    private final long value;

    private SetColumn(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public Long getValue() {
        return this.value;
    }

    public static final SetColumn valueOf(long value) {
        return new SetColumn(value);
    }
}
