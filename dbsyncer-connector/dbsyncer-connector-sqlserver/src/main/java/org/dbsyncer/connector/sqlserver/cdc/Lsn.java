/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.cdc;

import java.util.Arrays;

import org.dbsyncer.common.util.StringUtil;

/**
 * SQL Server LSN（日志序列号）位置的逻辑表示, LSN不可用时为NULL。
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public class Lsn implements Comparable<Lsn> {
    private static final String NULL_STRING = "NULL";

    public static final Lsn NULL = new Lsn(null);

    private final byte[] binary;
    private int[] unsignedBinary;

    private String string;

    public Lsn(byte[] binary) {
        this.binary = binary;
    }

    /**
     * @return binary representation of the stored LSN
     */
    public byte[] getBinary() {
        return binary;
    }

    /**
     * @return true if this is a real LSN or false it it is {@code NULL}
     */
    public boolean isAvailable() {
        return binary != null;
    }

    private int[] getUnsignedBinary() {
        if (unsignedBinary != null || binary == null) {
            return unsignedBinary;
        }

        unsignedBinary = new int[binary.length];
        for (int i = 0; i < binary.length; i++) {
            unsignedBinary[i] = Byte.toUnsignedInt(binary[i]);
        }
        return unsignedBinary;
    }

    /**
     * @return textual representation of the stored LSN
     */
    public String toString() {
        if (string != null) {
            return string;
        }
        final StringBuilder sb = new StringBuilder();
        if (binary == null) {
            return NULL_STRING;
        }
        final int[] unsigned = getUnsignedBinary();
        for (int i = 0; i < unsigned.length; i++) {
            final String byteStr = Integer.toHexString(unsigned[i]);
            if (byteStr.length() == 1) {
                sb.append('0');
            }
            sb.append(byteStr);
            if (i == 3 || i == 7) {
                sb.append(':');
            }
        }
        string = sb.toString();
        return string;
    }

    /**
     * @param lsnString - textual representation of Lsn
     * @return LSN converted from its textual representation
     */
    public static Lsn valueOf(String lsnString) {
        return (lsnString == null || NULL_STRING.equals(lsnString)) ? NULL : new Lsn(StringUtil.hexStringToByteArray(lsnString.replace(":", "")));
    }

    /**
     * @param lsnBinary - binary representation of Lsn
     * @return LSN converted from its binary representation
     */
    public static Lsn valueOf(byte[] lsnBinary) {
        return (lsnBinary == null) ? NULL : new Lsn(lsnBinary);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(binary);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Lsn other = (Lsn) obj;
        if (!Arrays.equals(binary, other.binary)) {
            return false;
        }
        return true;
    }

    /**
     * Enables ordering of LSNs. The {@code NULL} LSN is always the smallest one.
     */
    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
        }
        if (!this.isAvailable()) {
            if (!o.isAvailable()) {
                return 0;
            }
            return -1;
        }
        if (!o.isAvailable()) {
            return 1;
        }
        final int[] thisU = getUnsignedBinary();
        final int[] thatU = o.getUnsignedBinary();
        for (int i = 0; i < thisU.length; i++) {
            final int diff = thisU[i] - thatU[i];
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

}