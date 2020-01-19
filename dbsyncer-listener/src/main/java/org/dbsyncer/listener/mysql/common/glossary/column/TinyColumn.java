package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class TinyColumn implements Column {
    private static final long serialVersionUID = 3629858638897033423L;

    public static final int MIN_VALUE = -128;
    public static final int MAX_VALUE = 127;

    private static final TinyColumn[] CACHE = new TinyColumn[256];

    static {
        for (int i = MIN_VALUE; i <= MAX_VALUE; i++) {
            CACHE[i + 128] = new TinyColumn(i);
        }
    }

    private final int value;

    private TinyColumn(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public Integer getValue() {
        return this.value;
    }

    public static final TinyColumn valueOf(int value) {
        if (value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        return CACHE[value + 128];
    }
}
