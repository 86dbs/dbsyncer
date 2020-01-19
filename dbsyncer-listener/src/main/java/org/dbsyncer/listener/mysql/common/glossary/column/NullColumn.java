package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

public final class NullColumn implements Column {
    private static final long serialVersionUID = 3300119160243172731L;

    private static final NullColumn[] CACHE = new NullColumn[256];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new NullColumn(i);
        }
    }

    private final int type;

    private NullColumn(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return null;
    }

    public int getType() {
        return type;
    }

    public Object getValue() {
        return null;
    }

    public static final NullColumn valueOf(int type) {
        if (type < 0 || type >= CACHE.length) throw new IllegalArgumentException("invalid type: " + type);
        return CACHE[type];
    }
}
