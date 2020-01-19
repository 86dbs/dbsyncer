package org.dbsyncer.listener.mysql.common.glossary.column;

import org.dbsyncer.listener.mysql.common.glossary.Column;

import java.math.BigDecimal;

public final class DecimalColumn implements Column {
    private static final long serialVersionUID = -3798378473095594835L;

    private final BigDecimal value;
    private final int precision;
    private final int scale;

    private DecimalColumn(BigDecimal value, int precision, int scale) {
        this.value = value;
        this.scale = scale;
        this.precision = precision;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    public BigDecimal getValue() {
        return this.value;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public static final DecimalColumn valueOf(BigDecimal value, int precision, int scale) {
        if (precision < scale)
            throw new IllegalArgumentException("invalid precision: " + precision + ", scale: " + scale);
        return new DecimalColumn(value, precision, scale);
    }
}
