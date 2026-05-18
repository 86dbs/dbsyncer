/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema;

import org.dbsyncer.connector.sqlserver.SqlServerException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * sqlserver hierarchyid 类型转换
 */
public final class HierarchyIdBinaryCode {

    private HierarchyIdBinaryCode() {
    }

    /**
     * 根节点：零长度字节。
     */
    public static String parseToHierarchyPath(byte[] hierarchyIdBytes) {
        if (hierarchyIdBytes == null || hierarchyIdBytes.length == 0) {
            return "/";
        }
        try {
            BitReader bitReader = new BitReader(hierarchyIdBytes);
            List<List<Long>> levels = new ArrayList<>();
            outer:
            while (true) {
                List<Long> step = new ArrayList<>();
                while (true) {
                    BitPattern pattern = KnownPatterns.getPatternByPrefix(bitReader);
                    if (pattern == null) {
                        break outer;
                    }
                    long encoded = bitReader.read(pattern.bitLength);
                    DecodeResult dr = pattern.decode(encoded);
                    step.add(dr.value);
                    if (dr.last) {
                        break;
                    }
                }
                levels.add(step);
            }
            if (levels.isEmpty()) {
                return "/";
            }
            StringBuilder sb = new StringBuilder();
            for (List<Long> step : levels) {
                sb.append('/');
                for (int i = 0; i < step.size(); i++) {
                    if (i > 0) {
                        sb.append('.');
                    }
                    sb.append(step.get(i));
                }
            }
            sb.append('/');
            return sb.toString();
        } catch (RuntimeException e) {
            throw new SqlServerException("Failed to parse hierarchyid binary", e);
        }
    }

    private static final class DecodeResult {
        final long value;
        final boolean last;

        DecodeResult(long value, boolean last) {
            this.value = value;
            this.last = last;
        }
    }

    /**
     * 与 Microsoft.SqlServer.Types.SqlHierarchy.BitReader 一致：自左向右的高位优先位流。
     */
    private static final class BitReader {
        private final byte[] bytes;
        private int bitPosition;

        BitReader(byte[] bytes) {
            this.bytes = bytes;
        }

        int getRemaining() {
            return bytes.length * 8 - bitPosition;
        }

        long read(int numBits) {
            long v = peek(numBits);
            bitPosition += numBits;
            return v;
        }

        long peek(int numBits) {
            if (numBits == 0) {
                return 0;
            }
            if (numBits > 63) {
                throw new SqlServerException("hierarchyid peek bit count > 63: " + numBits);
            }
            int currentByte = bitPosition / 8;
            int newByte = (bitPosition + numBits - 1) / 8;

            if (currentByte == newByte) {
                int offset = (8 - bitPosition % 8) - numBits;
                long mask = (0xFFL >> (8 - numBits)) << offset;
                return (bytes[currentByte] & mask) >>> offset;
            }

            long result = 0;
            int startOffset = bitPosition % 8;
            int firstCompleteByte = startOffset == 0 ? currentByte : currentByte + 1;
            int endOffset = (bitPosition + numBits) % 8;
            int lastCompleteByte = endOffset == 0 ? newByte + 1 : newByte;

            if (startOffset > 0) {
                long startMask = 0xFFL >> startOffset;
                result = bytes[currentByte] & startMask;
            }

            for (int i = firstCompleteByte; i < lastCompleteByte; i++) {
                result = (result << 8) | (bytes[i] & 0xFFL);
            }

            if (endOffset > 0) {
                long endMask = (0xFFL >> (8 - endOffset)) << (8 - endOffset);
                result = (result << endOffset) | (((endMask & bytes[newByte]) & 0xFFL) >>> (8 - endOffset));
            }

            return result;
        }
    }

    /**
     * 与 Microsoft.SqlServer.Types.SqlHierarchy.BitPattern 一致。
     */
    private static final class BitPattern {
        final long minValue;
        final long maxValue;
        final int bitLength;
        final long patternOnes;
        final long patternMask;
        final long prefixOnes;
        final int prefixBitLength;

        BitPattern(long minValue, long maxValue, String pattern) {
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.bitLength = pattern.length();
            this.patternOnes = getBitMask(pattern, c -> c == '1');
            this.patternMask = getBitMask(pattern, c -> c == 'x');
            int firstX = pattern.indexOf('x');
            String prefix = firstX < 0 ? pattern : pattern.substring(0, firstX);
            this.prefixOnes = getBitMask(prefix, c -> c == '1');
            this.prefixBitLength = prefix.length();
        }

        private static long getBitMask(String pattern, Predicate<Character> isOne) {
            long result = 0;
            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);
                result = (result << 1) | (isOne.test(c) ? 1L : 0L);
            }
            return result;
        }

        DecodeResult decode(long encodedValue) {
            long decodedValue = compress(encodedValue, patternMask);
            boolean isLast = (encodedValue & 0x1L) == 0x1L;
            long v = (isLast ? decodedValue : decodedValue - 1) + minValue;
            return new DecodeResult(v, isLast);
        }

        private static long compress(long value, long mask) {
            if (mask == 0) {
                return 0;
            }
            if ((mask & 0x1L) != 0) {
                return (compress(value >>> 1, mask >>> 1) << 1) | (value & 0x1L);
            }
            return compress(value >>> 1, mask >>> 1);
        }
    }

    /**
     * 与 Microsoft.SqlServer.Types.SqlHierarchy.KnownPatterns 一致（正数 / 负数前缀表）。
     */
    private static final class KnownPatterns {

        private static final BitPattern[] POSITIVE = new BitPattern[]{
                new BitPattern(0, 3, "01xx1"),
                new BitPattern(4, 7, "100xx1"),
                new BitPattern(8, 15, "101xxx1"),
                new BitPattern(16, 79, "110xx0x1xxx1"),
                new BitPattern(80, 1103, "1110xxx0xxx0x1xxx1"),
                new BitPattern(1104, 5199, "11110xxxxx0xxx0x1xxx1"),
                new BitPattern(5200, 4294972495L, "111110xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxx1"),
                new BitPattern(4294972496L, 281479271683151L, "111111xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxx1"),
        };

        private static final BitPattern[] NEGATIVE = new BitPattern[]{
                new BitPattern(-8, -1, "00111xxx1"),
                new BitPattern(-72, -9, "0010xx0x1xxx1"),
                new BitPattern(-4168, -73, "000110xxxxx0xxx0x1xxx1"),
                new BitPattern(-4294971464L, -4169, "000101xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxx1"),
                new BitPattern(-281479271682120L, -4294971465L, "000100xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxx1"),
        };

        static BitPattern getPatternByPrefix(BitReader bitR) {
            int remaining = bitR.getRemaining();
            if (remaining == 0) {
                return null;
            }
            if (remaining < 8 && bitR.peek(remaining) == 0) {
                return null;
            }

            if ((bitR.peek(2) & 0x3L) == 0) {
                for (BitPattern pattern : NEGATIVE) {
                    if (pattern.bitLength > remaining) {
                        break;
                    }
                    if (pattern.prefixBitLength > remaining) {
                        continue;
                    }
                    if (pattern.prefixOnes == bitR.peek(pattern.prefixBitLength)) {
                        return pattern;
                    }
                }
                throw new SqlServerException("No negative hierarchyid pattern matches bit prefix");
            }

            for (BitPattern pattern : POSITIVE) {
                if (pattern.bitLength > remaining) {
                    break;
                }
                if (pattern.prefixBitLength > remaining) {
                    continue;
                }
                if (pattern.prefixOnes == bitR.peek(pattern.prefixBitLength)) {
                    return pattern;
                }
            }
            throw new SqlServerException("No positive hierarchyid pattern matches bit prefix");
        }
    }
}
