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
}
