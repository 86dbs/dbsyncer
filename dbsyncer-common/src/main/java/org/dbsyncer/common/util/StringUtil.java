package org.dbsyncer.common.util;

import org.apache.commons.lang3.StringUtils;

public abstract class StringUtil {

    public static final String EMPTY = "";

    public static final String SYMBOL = "-";

    public static final String COLON = ":";

    public static final String SPACE = " ";

    public static final String FORWARD_SLASH = "/";

    public static boolean equals(CharSequence cs1, CharSequence cs2) {
        return StringUtils.equals(cs1, cs2);
    }

    public static boolean equalsIgnoreCase(CharSequence cs1, CharSequence cs2) {
        return StringUtils.equalsIgnoreCase(cs1, cs2);
    }

    public static boolean isBlank(CharSequence cs) {
        return StringUtils.isBlank(cs);
    }

    public static boolean isNotBlank(CharSequence cs) {
        return StringUtils.isNotBlank(cs);
    }

    public static String[] split(String str, String separatorChars) {
        return StringUtils.split(str, separatorChars);
    }

    public static boolean contains(CharSequence seq, CharSequence searchSeq) {
        return StringUtils.contains(seq, searchSeq);
    }

    public static boolean endsWith(final CharSequence str, final CharSequence suffix) {
        return StringUtils.endsWith(str, suffix);
    }

    public static String trim(String text) {
        return StringUtils.trim(text);
    }

    public static String replace(String text, String searchString, String replacement) {
        return StringUtils.replace(text, searchString, replacement);
    }

    public static String join(Iterable<?> iterable, String separator) {
        return StringUtils.join(iterable, separator);
    }

    public static String join(Object[] array, String separator) {
        return StringUtils.join(array, separator);
    }

    public static String substring(String str, int start) {
        return StringUtils.substring(str, start);
    }

    public static String substring(String str, int start, int end) {
        return StringUtils.substring(str, start, end);
    }

    public static int indexOf(CharSequence seq, CharSequence searchChar) {
        return StringUtils.indexOf(seq, searchChar);
    }

    public static int lastIndexOf(CharSequence seq, CharSequence searchChar) {
        return StringUtils.lastIndexOf(seq, searchChar);
    }

    public static boolean startsWith(CharSequence str, CharSequence prefix) {
        return StringUtils.startsWith(str, prefix);
    }

    public static String toString(Object obj) {
        return obj == null ? "" : String.valueOf(obj);
    }

    /**
     * 首字母转小写
     *
     * @param s
     * @return
     */
    public static String toLowerCaseFirstOne(String s) {
        if (StringUtils.isBlank(s) || Character.isLowerCase(s.charAt(0))) {
            return s;
        }
        return new StringBuilder().append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
    }

    /**
     * Restores a byte array that is encoded as a hex string.
     */
    public static byte[] hexStringToByteArray(String hexString) {
        if (hexString == null) {
            return null;
        }

        int length = hexString.length();
        byte[] bytes = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return bytes;
    }

}