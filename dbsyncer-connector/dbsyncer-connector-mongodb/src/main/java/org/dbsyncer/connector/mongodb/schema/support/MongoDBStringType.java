/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.schema.support;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBStringType extends StringType {

    private enum TypeEnum {
        STRING("string"), OBJECT_ID("objectId"), OBJECT("object"), ARRAY("array"), BINARY("binData");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    /**
     * 读侧归一化：Binary/byte[] 保持为 byte[]，与 MySQL BLOB 标准域对齐，避免校验时 byte[] vs String。
     * 字段类型常为 string（空集合元数据推断），不能只靠 typeName=binData 判断。
     */
    @Override
    public Object mergeValue(Object val, Field field) {
        if (val == null) {
            return getDefaultMergedVal(field);
        }
        if (val instanceof Binary) {
            return ((Binary) val).getData();
        }
        if (val instanceof byte[]) {
            return val;
        }
        if (TypeEnum.BINARY.getValue().equals(field.getTypeName()) && val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        return super.mergeValue(val, field);
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof ObjectId) {
            return val.toString();
        }
        if (val instanceof Number) {
            return val.toString();
        }
        if (val instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) val);
        }
        if (val instanceof Date) {
            return DateFormatUtil.dateToString((Date) val);
        }
        if (val instanceof java.util.Date) {
            return DateFormatUtil.dateToString((java.util.Date) val);
        }
        if (val instanceof Boolean) {
            return val.toString();
        }
        if (val instanceof Map || val instanceof List) {
            return JsonUtil.objToJson(val);
        }
        return throwUnsupportedException(val, field);
    }

    /**
     * 写入前转换：MySQL BLOB/BINARY 等以 byte[] 到来时，无论目标字段声明为 string 还是 binData，
     * 都写入 BSON Binary，避免父类把 byte[] 转成 String 导致数据损坏。
     */
    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Binary) {
            return val;
        }
        if (val instanceof byte[]) {
            return new Binary((byte[]) val);
        }
        if (TypeEnum.BINARY.getValue().equals(field.getTypeName())) {
            if (val instanceof String) {
                return new Binary(((String) val).getBytes(StandardCharsets.UTF_8));
            }
            return throwUnsupportedException(val, field);
        }
        return super.convert(val, field);
    }
}
