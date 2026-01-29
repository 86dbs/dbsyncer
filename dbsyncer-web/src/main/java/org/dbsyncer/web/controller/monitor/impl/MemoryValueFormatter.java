package org.dbsyncer.web.controller.monitor.impl;

import org.dbsyncer.web.controller.monitor.ValueFormatter;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
public final class MemoryValueFormatter implements ValueFormatter<Object, Object> {

    @Override
    public Object formatValue(Object value) {
        BigDecimal decimal = new BigDecimal(String.valueOf(value));
        BigDecimal bt = divide(decimal, 0);
        return divide(bt, 2);
    }

    private BigDecimal divide(BigDecimal d1, int scale) {
        return d1.divide(new BigDecimal("1024"), scale, RoundingMode.HALF_UP);
    }
}
