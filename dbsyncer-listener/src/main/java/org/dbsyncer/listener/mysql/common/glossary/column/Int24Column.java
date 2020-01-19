package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class Int24Column implements Column {
    private static final long serialVersionUID = 6456161237369680803L;

    public static final int MIN_VALUE = -8388608;
    public static final int MAX_VALUE = 8388607;

    private static final Int24Column[] CACHE = new Int24Column[255];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new Int24Column(i + Byte.MIN_VALUE);
        }
    }

    private final int value;

    private Int24Column(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public Integer getValue() {
        return this.value;
    }

    public static final Int24Column valueOf(int value) {
        if (value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        final int index = value - Byte.MIN_VALUE;
        return (index >= 0 && index < CACHE.length) ? CACHE[index] : new Int24Column(value);
    }
}
