/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.monitor.impl;

import org.dbsyncer.web.controller.monitor.ValueFormatter;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-12-21 00:29
 */
@Service
public final class GBValueFormatter implements ValueFormatter<Object, BigDecimal> {

    @Override
    public BigDecimal formatValue(Object value) {
        BigDecimal decimal = new BigDecimal(String.valueOf(value));
        BigDecimal bt = divide(decimal, 0);
        BigDecimal mb = divide(bt, 0);
        return divide(mb, 2);
    }

    private BigDecimal divide(BigDecimal d1, int scale) {
        return d1.divide(new BigDecimal("1024"), scale, RoundingMode.HALF_UP);
    }
}
