/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.schema.OracleLobParameter;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦字符类型（含 MySQL 同构迁移遗留的 TEXT/JSON 别名）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public final class DamengStringType extends StringType {

    private enum TypeEnum {
        VARCHAR("VARCHAR"),
        CHARACTER("CHARACTER"),
        NVARCHAR("NVARCHAR"),
        TEXT("TEXT"),
        TINYTEXT("TINYTEXT"),
        MEDIUMTEXT("MEDIUMTEXT"),
        LONGTEXT("LONGTEXT"),
        LONGVARCHAR("LONGVARCHAR"),
        JSON("JSON");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        static boolean isLob(String typeName) {
            if (StringUtil.isBlank(typeName)) {
                return false;
            }
            String t = typeName.trim().toUpperCase(Locale.ROOT);
            return t.contains("TEXT") || "LONGVARCHAR".equals(t) || "JSON".equals(t) || t.contains("CLOB");
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        if (val instanceof Number || val instanceof Boolean || val instanceof Character) {
            return String.valueOf(val);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        Object converted = super.convert(val, field);
        // MERGE 大字段需以 CLOB 绑定，配合 CAST(? AS CLOB)
        if (converted instanceof String && TypeEnum.isLob(field.getTypeName())) {
            return new OracleLobParameter((String) converted, field);
        }
        return converted;
    }
}
