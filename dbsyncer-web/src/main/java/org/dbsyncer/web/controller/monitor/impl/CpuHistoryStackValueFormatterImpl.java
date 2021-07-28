package org.dbsyncer.web.controller.monitor.impl;

import org.dbsyncer.web.controller.monitor.HistoryStackValueFormatter;
import org.springframework.stereotype.Service;

@Service
public class CpuHistoryStackValueFormatterImpl implements HistoryStackValueFormatter {

    @Override
    public Object formatValue(Object value) {
        Double val = (Double) value;
        val *= 100;
        String percent = String.format("%.2f", val);
        return percent;
    }

}
