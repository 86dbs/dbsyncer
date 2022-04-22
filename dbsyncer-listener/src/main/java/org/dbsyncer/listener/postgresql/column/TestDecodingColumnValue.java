package org.dbsyncer.listener.postgresql.column;

public final class TestDecodingColumnValue extends AbstractColumnValue {

    private String value;

    public TestDecodingColumnValue(String value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public Integer asInteger() {
        return null;
    }

    @Override
    public Long asLong() {
        return null;
    }

    @Override
    public Float asFloat() {
        return null;
    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public Object asDecimal() {
        return null;
    }

    @Override
    public byte[] asByteArray() {
        return new byte[0];
    }
}