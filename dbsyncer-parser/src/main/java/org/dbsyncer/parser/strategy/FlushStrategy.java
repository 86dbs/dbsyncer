package org.dbsyncer.parser.strategy;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

import java.util.Map;

/**
 * 记录同步数据策略
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
public interface FlushStrategy {

    /**
     * 记录全量同步数据
     *
     * @param result 同步结果
     * @param targetSchemaResolver 目标字段解析器
     * @param targetFieldMap 目标表字段集合
     */
    void flushFullData(Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap);

    /**
     * 记录增量同步数据
     *
     * @param result 同步结果
     * @param targetSchemaResolver 目标字段解析器
     * @param targetFieldMap 目标表字段集合
     */
    void flushIncrementData(Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap);

    /**
     * 仅写入成功/失败明细到存储，不刷新 Meta 计数。
     * <p>供已单独汇总成功/失败计数的路径使用，避免与 Meta 双计。
     * 失败明细受「记录同步失败数据」开关控制；成功明细需同时开启「记录全量数据」与「记录同步成功数据」。
     *
     * @param result               同步结果（须已设置 taskId，明细分表键）
     * @param targetSchemaResolver 目标字段解析器
     * @param targetFieldMap       目标表字段集合
     */
    void writeStorageOnly(Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap);
}
