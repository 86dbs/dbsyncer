/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server 精确数值类型
 * 包括整数类型和精确小数类型
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerExactNumericType extends LongType {

    private enum TypeEnum {
        // 整数类型
        BIT,           // 位类型 (0 或 1)
        TINYINT,       // 1字节整数 (0-255)
        SMALLINT,      // 2字节整数 (-32,768 到 32,767)
        INT,           // 4字节整数 (-2,147,483,648 到 2,147,483,647)
        BIGINT,        // 8字节整数 (-9,223,372,036,854,775,808 到 9,223,372,036,854,775,807)
        // IDENTITY 类型 (自增列)
        INT_IDENTITY("INT IDENTITY"),  // INT IDENTITY 自增列
        BIGINT_IDENTITY("BIGINT IDENTITY"), // BIGINT IDENTITY 自增列
        SMALLINT_IDENTITY("SMALLINT IDENTITY"), // SMALLINT IDENTITY 自增列
        TINYINT_IDENTITY("TINYINT IDENTITY");  // TINYINT IDENTITY 自增列

        private final String typeName;
        
        TypeEnum() {
            this.typeName = name();
        }
        
        TypeEnum(String typeName) {
            this.typeName = typeName;
        }
        
        public String getTypeName() {
            return typeName;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getTypeName).collect(Collectors.toSet());
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof String) {
            try {
                return Long.parseLong((String) val);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        return 0L;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Long) {
            return val;
        }
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof BigDecimal) {
            return ((BigDecimal) val).longValue();
        }
        return super.convert(val, field);
    }
}