/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.schema;

import org.dbsyncer.connector.oceanbase.OceanBaseException;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseByteType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseBytesType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDateType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDecimalType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDoubleType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseFloatType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseIntType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseLongType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseShortType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseStringType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseTimeType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * OceanBase标准数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-04 00:20
 */
public final class OceanBaseSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new OceanBaseBytesType(),
            new OceanBaseByteType(),
            new OceanBaseDateType(),
            new OceanBaseDecimalType(),
            new OceanBaseDoubleType(),
            new OceanBaseFloatType(),
            new OceanBaseIntType(),
            new OceanBaseLongType(),
            new OceanBaseShortType(),
            new OceanBaseStringType(),
            new OceanBaseTimestampType(),
            new OceanBaseTimeType())
        .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            if (mapping.containsKey(typeName)) {
                throw new OceanBaseException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

}