package org.dbsyncer.web.controller.monitor.impl;

import org.dbsyncer.web.controller.monitor.ValueFormatter;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
public final class CpuValueFormatter implements ValueFormatter<Object, Object> {

    @Override
    public Object formatValue(Object value) {
        Double val = (Double) value;
        if (Double.isNaN(val)) {
            return 0.0;
        }
        val *= 100;
        String percent = String.format("%.2f", val);
        return new BigDecimal(percent).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

}
