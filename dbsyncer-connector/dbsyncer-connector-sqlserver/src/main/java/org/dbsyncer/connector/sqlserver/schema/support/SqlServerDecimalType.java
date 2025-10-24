/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server Decimal类型支持
 * 专门处理DECIMAL和NUMERIC类型，保持精度和小数位数
 *
 * @Author Qwen
 * @Version 1.0.0
 * @Date 2025-10-24
 */
public final class SqlServerDecimalType extends DecimalType {

    private enum TypeEnum {
        DECIMAL,       // 精确小数
        NUMERIC,       // 数值类型 (DECIMAL 的同义词)
        DECIMAL_IDENTITY("DECIMAL IDENTITY"),  // DECIMAL IDENTITY 自增列
        NUMERIC_IDENTITY("NUMERIC IDENTITY"),   // NUMERIC IDENTITY 自增列
        // 货币类型
        MONEY,         // 货币类型 (8字节)
        SMALLMONEY;    // 小货币类型 (4字节)

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
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return (BigDecimal) val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            try {
                return new BigDecimal((String) val);
            } catch (NumberFormatException e) {
                return BigDecimal.ZERO;
            }
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return BigDecimal.ZERO;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            try {
                return new BigDecimal((String) val);
            } catch (NumberFormatException e) {
                return BigDecimal.ZERO;
            }
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return super.convert(val, field);
    }
}