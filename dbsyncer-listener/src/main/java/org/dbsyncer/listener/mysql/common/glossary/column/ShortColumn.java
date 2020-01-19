package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class ShortColumn implements Column {
    private static final long serialVersionUID = 2278283065371267842L;

    public static final int MIN_VALUE = Short.MIN_VALUE;
    public static final int MAX_VALUE = Short.MAX_VALUE;

    private static final ShortColumn[] CACHE = new ShortColumn[255];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new ShortColumn(i + Byte.MIN_VALUE);
        }
    }

    private final int value;

    private ShortColumn(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public Integer getValue() {
        return this.value;
    }

    public static final ShortColumn valueOf(int value) {
        if (value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        final int index = value - Byte.MIN_VALUE;
        return (index >= 0 && index < CACHE.length) ? CACHE[index] : new ShortColumn(value);
    }
}
