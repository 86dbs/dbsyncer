package org.dbsyncer.common.util;

import org.apache.commons.lang3.RandomUtils;

public abstract class RandomUtil {

    public static int nextInt(int startInclusive, int endExclusive) {
        return RandomUtils.nextInt(startInclusive, endExclusive);
    }
}
