/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.storage.enums;

/**
 * 支持的序列化/反序列化字节码类型
 *
 * <pre>
 * 类型     长度     大小      最小值     最大值
 * byte     1Byte    8-bit     -128       +127
 * short    2Byte    16-bit    -2^15      +2^15-1
 * int      4Byte    32-bit    -2^31      +2^31-1
 * long     8Byte    64-bit    -2^63      +2^63-1
 * float    4Byte    32-bit    IEEE754    IEEE754
 * double   8Byte    64-bit    IEEE754    IEEE754
 * char     2Byte    16-bit    Unicode 0  Unicode 2^16-1
 * boolean  8Byte    64-bit
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/4/21 22:07
 */
public enum BinlogByteEnum {

    /**
     * 8Byte
     */
    LONG(8),
    /**
     * 8Byte
     */
    DOUBLE(8),
    /**
     * 4Byte
     */
    INTEGER(4),
    /**
     * 4Byte
     */
    FLOAT(4),
    /**
     * 2Byte
     */
    SHORT(2),
    /**
     * 1Byte
     */
    BYTE(1),;

    BinlogByteEnum(int byteLength) {
        this.byteLength = byteLength;
    }

    final int byteLength;

    public int getByteLength() {
        return byteLength;
    }
}
