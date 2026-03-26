/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 字符串类型
 * 支持: keyword, text, string(deprecated), ip, object, geo_point, geo_shape
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchStringType extends StringType {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchStringType.class);

    private enum TypeEnum {

        KEYWORD("keyword"), WILDCARD("wildcard"), TEXT("text"), @Deprecated
        STRING("string"), // ES 5.X之后不再支持，由text或keyword取代
        IP("ip"), IP_RANGE("ip_range"), INTEGER_RANGE("integer_range"), LONG_RANGE("long_range"), DOUBLE_RANGE("double_range"), FLOAT_RANGE("float_range"), DATE_RANGE("date_range"),
        COMPLETION("completion"), OBJECT("object"), FLATTENED("flattened"), /* 7.3+ 引入的一种特殊数据类型，用于扁平化对象（Flattened Object）存储。它专门用于处理动态的、不可预测结构的对象字段，通过将整个对象扁平化为键值对来避免字段爆炸问题。 */
        DENSE_VECTOR("dense_vector"), /* 7.0+ 引入的密集向量, 用于存储和搜索高维浮点数向量, 支持现代 AI/ML 应用的向量相似性搜索。 */
        SHAPE("shape"), RANK_FEATURES("rank_features"), /* 7.8+ 专门用于特征排名 */
        POINT("point"), GEO_POINT("geo_point"), GEO_SHAPE("geo_shape"), NESTED("nested");

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

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
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

    @Override
    public Object convertValue(Object val, Field field) {
        if (val instanceof String) {
            // 将类型名转大写并替换空格为下划线，以匹配 TypeEnum
            String enumName = field.getTypeName().toUpperCase().replace(" ", "_");
            TypeEnum typeEnum = TypeEnum.valueOf(enumName);
            switch (typeEnum) {
                case OBJECT:
                case FLATTENED:
                case COMPLETION:
                case IP_RANGE:
                case INTEGER_RANGE:
                case LONG_RANGE:
                case DOUBLE_RANGE:
                case FLOAT_RANGE:
                case DATE_RANGE:
                case RANK_FEATURES:
                case GEO_POINT:
                case POINT:
                case NESTED:
                case DENSE_VECTOR:
                case KEYWORD:
                    return parseObject((String) val);
                default:
                    break;
            }
        }
        return super.convertValue(val, field);
    }

    private Object parseObject(String val) {
        JsonUtil.JsonType type = JsonUtil.getJsonType(val);
        switch (type) {
            case STRING:
                return val;
            case OBJECT:
                return JsonUtil.jsonToObj(val, Map.class);
            case ARRAY:
                return JsonUtil.jsonToObj(val, List.class);
            default:
                break;
        }
        return val;
    }
}
