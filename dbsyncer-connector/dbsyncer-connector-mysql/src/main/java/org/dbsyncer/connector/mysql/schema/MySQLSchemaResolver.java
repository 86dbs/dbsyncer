/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MySQL标准数据类型解析器
 * <p>https://gitee.com/ghi/dbsyncer/wikis/%E9%A1%B9%E7%9B%AE%E8%AE%BE%E8%AE%A1/%E6%A0%87%E5%87%86%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B/MySQL</p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-25 22:08
 */
public final class MySQLSchemaResolver implements SchemaResolver {

    private final Map<DataTypeEnum, DataType> STANDARD_TYPES = new LinkedHashMap<>();
    private final Map<String, DataType> DATA_TYPES = new LinkedHashMap<>();

    private MySQLSchemaResolver() {
        DATA_TYPES.putIfAbsent("VARCHAR", new StringType());
        // TODO
    }

    @Override
    public Object convert(Object val, Field field) {
        if (DATA_TYPES.containsKey(field.getTypeName())) {
            DataType dataType = DATA_TYPES.get(field.getTypeName());
            if (STANDARD_TYPES.containsKey(dataType.getType())) {
                return STANDARD_TYPES.get(dataType.getType()).convert(val);
            }
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convertValue(Object val, Field field) {
        if (DATA_TYPES.containsKey(field.getTypeName())) {
            return DATA_TYPES.get(field.getTypeName()).convert(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

}