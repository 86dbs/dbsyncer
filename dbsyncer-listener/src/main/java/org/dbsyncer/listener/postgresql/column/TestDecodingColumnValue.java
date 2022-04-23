package org.dbsyncer.listener.postgresql.column;

import org.dbsyncer.common.util.StringUtil;

import java.math.BigDecimal;

public final class TestDecodingColumnValue extends AbstractColumnValue {

    private String value;

    public TestDecodingColumnValue(String value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return "t".equalsIgnoreCase(value);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Long asLong() {
        return Long.valueOf(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    @Override
    public Object asDecimal() {
        return new BigDecimal(value);
    }

    @Override
    public byte[] asByteArray() {
        return StringUtil.hexStringToByteArray(value.substring(2));
    }
}