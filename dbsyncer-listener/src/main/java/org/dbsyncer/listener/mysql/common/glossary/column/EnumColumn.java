package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class EnumColumn implements Column {
    private static final long serialVersionUID = -6017298545673303080L;

    private final int value;

    private EnumColumn(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public Integer getValue() {
        return this.value;
    }

    public static final EnumColumn valueOf(int value) {
        return new EnumColumn(value);
    }
}
