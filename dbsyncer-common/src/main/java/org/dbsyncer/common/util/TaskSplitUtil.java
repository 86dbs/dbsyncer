/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-10 17:47
 */
public final class TaskSplitUtil {

    public static <T> void split(List<T> rows, int batchNum, Consumer<List<T>> consumer) {
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }
        int effectiveBatch = batchNum > 0 ? batchNum : rows.size();
        int total = rows.size();
        for (int from = 0; from < total; from += effectiveBatch) {
            consumer.accept(rows.stream().skip(from).limit(effectiveBatch).collect(Collectors.toList()));
        }
    }
}
