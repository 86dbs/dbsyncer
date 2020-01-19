package org.dbsyncer.listener.mysql.common.util;

import java.math.BigDecimal;
import java.util.Calendar;

public final class MySQLUtils {
	private static final int DIGITS_PER_4BYTES = 9;
	private static final BigDecimal POSITIVE_ONE = BigDecimal.ONE;
	private static final BigDecimal NEGATIVE_ONE = new BigDecimal("-1");
	private static final int DECIMAL_BINARY_SIZE[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

	public static byte[] password41OrLater(byte password[], byte scramble[]) {
		final byte[] stage1 = CodecUtils.sha(password);
		final byte[] stage2 = CodecUtils.sha(CodecUtils.concat(scramble, CodecUtils.sha(stage1)));
		return CodecUtils.xor(stage1, stage2);
	}

	public static int toYear(int value) {
		return 1900 + value;
	}

	public static java.sql.Date toDate(int value) {
		final int d = value % 32; value >>>= 5;
		final int m = value % 16;
		final int y = value >> 4;
		final Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(y, m - 1, d);
		return new java.sql.Date(cal.getTimeInMillis());
	}

	public static java.sql.Time toTime(int value) {
		final int s = (int)(value % 100); value /= 100;
		final int m = (int)(value % 100);
		final int h = (int)(value / 100);
		final Calendar c = Calendar.getInstance();
        c.set(1970, 0, 1, h, m, s);
        c.set(Calendar.MILLISECOND, 0);
        return new java.sql.Time(c.getTimeInMillis());
	}

	public static java.sql.Time toTime2(int value, int fraction, int width) {
		final long millis = getMillisFromTime2(value);
		return new java.sql.Time(millis + (nanosForFractionalValue(fraction, width) / 1000000));
	}

	public static long getMillisFromTime2(int value) {
		final int h = (value >> 12) & 0x3FF;
		final int m = (value >> 6) & 0x3F;
		final int s = (value >> 0) & 0x3F;
		final Calendar c = Calendar.getInstance();
		c.set(1970, 0, 1, h, m, s);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTimeInMillis();
	}

	public static java.sql.Timestamp time2toTimestamp(int value, int fraction, int width) {
		final long millis = getMillisFromTime2(value);
		final java.sql.Timestamp t = new java.sql.Timestamp(millis);
		t.setNanos(nanosForFractionalValue(fraction, width));
		return t;
	}

	// fractional values are 0 - 3 bytes wide.
	// 1 byte, we get 0-99 resolution
	// 2 bytes, we get 0-9,999
	// 3 bytes, we get 0-999,999 resolution

	// 1 byte: the value "99" means 990,000,000 nanos == fraction * 10^7
	// 2 bytes: the value "9,999" means 999,900,000 nanos == fraction * 10^5
	// 3 bytes: the value "999,999" means "999,999,000" nanos == fraction * 10^3
	public static int nanosForFractionalValue(int value, int width) {
		switch (width) {
			case 0:
				return 0;
			case 1:
				return value * 10000000;
			case 2:
				return value * 100000;
			case 3:
				return value * 1000;
			default:
				throw new RuntimeException("unexpected number of fractional bytes");
		}
	}

	public static java.util.Date toDatetime(long value) {
		final int second = (int)(value % 100); value /= 100;
		final int minute = (int)(value % 100); value /= 100;
		final int hour = (int)(value % 100); value /= 100;
		final int day = (int)(value % 100); value /= 100;
		final int month = (int)(value % 100);
		final int year = (int)(value / 100);
		final Calendar c = Calendar.getInstance();
        c.set(year, month - 1, day, hour, minute, second);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
	}

	public static java.util.Date toDatetime2(long value, int fraction, int width) {
		final long millis = getMillisFromDatetime2(value);
		return new java.util.Date(millis + (nanosForFractionalValue(fraction, width) / 1000000));
	}

	public static long getMillisFromDatetime2(long value) {
		final long x = (value >> 22) & 0x1FFFFL;
		final int year = (int)(x / 13);
		final int month = (int)(x % 13);
		final int day = ((int)(value >> 17)) & 0x1F;
		final int hour = ((int)(value >> 12)) & 0x1F;
		final int minute = ((int)(value >> 6)) & 0x3F;
		final int second = ((int)(value >> 0)) & 0x3F;

		final Calendar c = Calendar.getInstance();
		c.set(year, month - 1, day, hour, minute, second);
		c.set(Calendar.MILLISECOND, 0);

		return c.getTimeInMillis();
	}

	public static java.sql.Timestamp toTimestamp(long seconds) {
		return new java.sql.Timestamp(seconds * 1000L);
	}

	public static java.sql.Timestamp toTimestamp2(long seconds, int fraction, int width) {
		return timestamp2ToTimestamp(seconds, fraction, width);
	}

	public static java.sql.Timestamp timestamp2ToTimestamp(long seconds, int fraction, int width) {
		final java.sql.Timestamp r = new java.sql.Timestamp(seconds * 1000L);
		r.setNanos(nanosForFractionalValue(fraction, width));
		return r;
	}

	public static java.sql.Timestamp datetime2ToTimestamp(long value, int fraction, int width) {
		final long millis = getMillisFromDatetime2(value);
		final java.sql.Timestamp r = new java.sql.Timestamp(millis);
		r.setNanos(nanosForFractionalValue(fraction, width));
		return r;
	}

	public static BigDecimal toDecimal(int precision, int scale, byte[] value) {
        final boolean positive = (value[0] & 0x80) == 0x80;
        value[0] ^= 0x80;
        if (!positive) {
        	 for (int i = 0; i < value.length; i++) {
        		 value[i] ^= 0xFF;
             }
        }

        final int x = precision - scale;
        final int ipDigits = x / DIGITS_PER_4BYTES;
        final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
        final int ipSize = (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX];
        int offset = DECIMAL_BINARY_SIZE[ipDigitsX];
        BigDecimal ip = offset > 0 ? BigDecimal.valueOf(CodecUtils.toInt(value, 0, offset)) : BigDecimal.ZERO;
        for(; offset < ipSize; offset += 4) {
        	final int i = CodecUtils.toInt(value, offset, 4);
        	ip = ip.movePointRight(DIGITS_PER_4BYTES).add(BigDecimal.valueOf(i));
        }

        int shift = 0;
        BigDecimal fp = BigDecimal.ZERO;
        for (; shift + DIGITS_PER_4BYTES <= scale; shift += DIGITS_PER_4BYTES, offset += 4) {
            final int i = CodecUtils.toInt(value, offset, 4);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIGITS_PER_4BYTES));
        }
        if(shift < scale) {
        	final int i = CodecUtils.toInt(value, offset, DECIMAL_BINARY_SIZE[scale - shift]);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
        }

        return positive ? POSITIVE_ONE.multiply(ip.add(fp)) : NEGATIVE_ONE.multiply(ip.add(fp));
	}

	public static int getDecimalBinarySize(int precision, int scale) {
		final int x = precision - scale;
        final int ipDigits = x / DIGITS_PER_4BYTES;
        final int fpDigits = scale / DIGITS_PER_4BYTES;
        final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
        final int fpDigitsX = scale - fpDigits * DIGITS_PER_4BYTES;
        return (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX] + (fpDigits << 2) + DECIMAL_BINARY_SIZE[fpDigitsX];
    }
}
