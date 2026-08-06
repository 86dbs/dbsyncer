/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks.schema.support;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Base64;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksStringType extends StringType {

    private enum TypeEnum {
        STRING, JSON, HLL, BITMAP, ARRAY, MAP, STRUCT, PERCENTILE, VARCHAR;

        public String getValue() {
            return name();
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val == null) {
            return null;
        }
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            // 目标为字符串列时，byte[] 转 Base64，避免 new String(bytes) 乱码或 "[B@..."
            return Base64.getEncoder().encodeToString((byte[]) val);
        }
        return JsonUtil.objToJsonSafe(val);
    }

    /**
     * 写入前转换：STRING/VARCHAR 等目标列收到 byte[] 时转 Base64，禁止 Object.toString 变成 "[B@xxxx]"。
     */
    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) val);
        }
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
}
