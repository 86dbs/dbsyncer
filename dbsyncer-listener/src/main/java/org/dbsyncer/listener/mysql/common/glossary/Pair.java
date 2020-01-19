package org.dbsyncer.listener.mysql.common.glossary;

import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public final class Pair<T> {
    private T before;
    private T after;

    public Pair() {
    }

    public Pair(T before, T after) {
        this.before = before;
        this.after = after;
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append("before", before)
                .append("after", after).toString();
    }

    public T getBefore() {
        return before;
    }

    public void setBefore(T before) {
        this.before = before;
    }

    public T getAfter() {
        return after;
    }

    public void setAfter(T after) {
        this.after = after;
    }

    public void swap() {
        final T t = this.before;
        this.before = this.after;
        this.after = t;
    }

    public static void swap(Pair<?> p) {
        doSwap(p); // Nothing but capture the <?>
    }

    private static <T> void doSwap(Pair<T> p) {
        synchronized (p) {
            final T t = p.before;
            p.before = p.after;
            p.after = t;
        }
    }
}
