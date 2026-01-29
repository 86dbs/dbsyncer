/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema;

import org.dbsyncer.connector.elasticsearch.ElasticsearchException;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchBooleanType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchByteType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchBytesType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchTimestampType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchDecimalType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchDoubleType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchFloatType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchIntType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchLongType;
import org.dbsyncer.connector.elasticsearch.schema.support.ElasticsearchStringType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Elasticsearch 异构数据类型解析器
 * 支持 ES 所有标准数据类型的映射和转换
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-13 00:16
 */
public final class ElasticsearchSchemaResolver extends AbstractSchemaResolver {
    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new ElasticsearchStringType(),
                new ElasticsearchIntType(),
                new ElasticsearchLongType(),
                new ElasticsearchDecimalType(),
                new ElasticsearchFloatType(),
                new ElasticsearchDoubleType(),
                new ElasticsearchTimestampType(),
                new ElasticsearchBooleanType(),
                new ElasticsearchBytesType(),
                new ElasticsearchByteType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new ElasticsearchException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}