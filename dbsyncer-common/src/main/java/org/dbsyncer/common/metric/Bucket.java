/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.metric;

import java.util.concurrent.atomic.LongAdder;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-06-02 22:45
 */
public final class Bucket {

    private LongAdder longAdder = new LongAdder();

    public void add(long count) {
        longAdder.add(count);
    }

    public long get() {
        return longAdder.sum();
    }

    public void reset() {
        this.longAdder.reset();
    }
}