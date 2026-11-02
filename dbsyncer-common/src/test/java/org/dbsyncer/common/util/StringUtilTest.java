/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilTest {
    @Test
    public void hexStringToByteArrayAcceptsValidValues() {
        Assert.assertNull(StringUtil.hexStringToByteArray(null));
        Assert.assertArrayEquals(new byte[0], StringUtil.hexStringToByteArray(""));
        Assert.assertArrayEquals(new byte[]{0, 15, (byte) 0xab, (byte) 0xff},
                StringUtil.hexStringToByteArray("000faBFF"));
    }

    @Test
    public void hexStringToByteArrayRejectsInvalidValues() {
        for (String value : new String[]{"0g", "g0"}) {
            try {
                StringUtil.hexStringToByteArray(value);
                Assert.fail("Expected IllegalArgumentException for: " + value);
            } catch (IllegalArgumentException expected) {
                // Invalid input must never silently produce a byte value.
            }
        }
    }

    @Test
    public void hexStringToByteArrayRejectsOddLength() {
        try {
            StringUtil.hexStringToByteArray("123");
            Assert.fail("Expected IllegalArgumentException for odd length");
        } catch (IllegalArgumentException expected) {
            // Odd-length input is not a sequence of complete bytes.
        }
    }
}
