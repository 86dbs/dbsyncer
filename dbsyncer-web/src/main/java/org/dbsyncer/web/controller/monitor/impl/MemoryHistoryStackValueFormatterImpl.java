package org.dbsyncer.web.controller.monitor.impl;

import org.dbsyncer.web.controller.monitor.HistoryStackValueFormatter;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class MemoryHistoryStackValueFormatterImpl implements HistoryStackValueFormatter {

    @Override
    public Object formatValue(Object value) {
        BigDecimal decimal = new BigDecimal(String.valueOf(value));
        BigDecimal bt = divide(decimal,0);
        BigDecimal mb = divide(bt,2);
        return mb;
    }

    private BigDecimal divide(BigDecimal d1, int scale) {
        return d1.divide(new BigDecimal("1024"), scale, BigDecimal.ROUND_HALF_UP);
    }
}
