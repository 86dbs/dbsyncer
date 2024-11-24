/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.math.BigDecimal;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public class DecimalType extends AbstractDataType<BigDecimal> {

    // 精度：表示数值的总位数，包括小数点前后的位数。例如，数值 123.45 的精度是 5，因为它有 5 位数字。
    private final int precision;

    // 刻度：表示小数点后的位数。例如，数值 123.45 的刻度是 2，因为小数点后有 2 位数字。
    private final int scale;

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.DECIMAL;
    }
}
