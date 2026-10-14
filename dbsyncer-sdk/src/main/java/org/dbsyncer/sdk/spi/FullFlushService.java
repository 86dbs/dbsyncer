/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Task;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.Map;

/**
 * 全量页级结果落库。集群和单机实现差异
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14
 */
public interface FullFlushService {

    /**
     * 全量页级结果落库。
     *
     * @param task                 运行态（分片执行时 id 为分片计划 ID）
     * @param result               本批写结果
     * @param targetSchemaResolver 目标库类型解析
     * @param targetFieldMap       目标字段
     */
    void flush(Task task, Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap);
}
