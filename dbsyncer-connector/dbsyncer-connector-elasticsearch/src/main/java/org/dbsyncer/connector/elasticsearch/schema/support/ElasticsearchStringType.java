/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
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

    private enum TypeEnum {
        KEYWORD("keyword"),
        TEXT("text"),
        @Deprecated
        STRING("string"),  // ES 5.X之后不再支持，由text或keyword取代
        IP("ip"),
        OBJECT("object"),
        GEO_POINT("geo_point"),
        GEO_SHAPE("geo_shape"),
        NESTED("nested");

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

        return throwUnsupportedException(val, field);
    }
}
