package org.dbsyncer.listener.mysql.common.glossary;

import org.dbsyncer.listener.mysql.common.util.CodecUtils;

import java.math.BigInteger;

public abstract class UnsignedLong extends Number implements Comparable<UnsignedLong> {
    private static final long serialVersionUID = -2263391850849681361L;

    private static final UnsignedLong[] CACHE = new UnsignedLong[255];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new UnsignedLong4(i);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UnsignedLong)) return false;
        return this.doubleValue() == ((UnsignedLong) obj).doubleValue();
    }

    public int compareTo(UnsignedLong rhs) {
        return (this.doubleValue() < rhs.doubleValue()) ? -1 : ((this.doubleValue() == rhs.doubleValue()) ? 0 : 1);
    }

    public static final UnsignedLong valueOf(long value) {
        if (value < 0) { // Convert to positive value
            return new UnsignedLongUnlimited(value);
        } else if (value < CACHE.length) {
            return CACHE[(int) value];
        } else if (value < Integer.MAX_VALUE) {
            return new UnsignedLong4((int) value);
        } else { // value < Long.MAX_VALUE)
            return new UnsignedLong8(value);
        }
    }

    private static final class UnsignedLong4 extends UnsignedLong {
        private static final long serialVersionUID = 6549354506227481646L;

        private final int value;

        private UnsignedLong4(int value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            return value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public int hashCode() {
            return this.value;
        }
    }

    private static final class UnsignedLong8 extends UnsignedLong {
        private static final long serialVersionUID = -314206857441911721L;

        private final long value;

        private UnsignedLong8(long value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }

    private static final class UnsignedLongUnlimited extends UnsignedLong {
        private static final long serialVersionUID = -5362638763306527191L;

        private final BigInteger value;

        private UnsignedLongUnlimited(long value) {
            this.value = new BigInteger(1, CodecUtils.toByteArray(value));
        }

        @Override
        public int intValue() {
            return value.intValue();
        }

        @Override
        public long longValue() {
            return value.longValue();
        }

        @Override
        public float floatValue() {
            return value.floatValue();
        }

        @Override
        public double doubleValue() {
            return value.doubleValue();
        }

        @Override
        public String toString() {
            return value.toString();
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
