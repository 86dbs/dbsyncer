package org.dbsyncer.common.column;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/24 18:22
 */
public final class Lexer {

    private final char[] array;
    private final int length;
    private int pos = 0;
    private String token;

    public Lexer(String input) {
        this.array = input.toCharArray();
        this.length = this.array.length;
    }

    public String token() {
        return token;
    }

    public String nextToken(char comma) {
        if (pos < length) {
            StringBuilder out = new StringBuilder(16);
            while (pos < length && array[pos] != comma) {
                out.append(array[pos]);
                pos++;
            }
            pos++;
            return token = out.toString();
        }
        return token = null;
    }

    public String nextTokenToQuote() {
        if (pos < length) {
            int commaCount = 1;
            StringBuilder out = new StringBuilder(16);
            while (!((pos == length - 1 || (array[pos + 1] == ' ' && commaCount % 2 == 1)) && array[pos] == '\'')) {
                if (array[pos] == '\'') {
                    commaCount++;
                }
                out.append(array[pos]);
                pos++;
            }
            pos++;
            return token = out.toString();
        }
        return token = null;
    }

    public void skip(int skip) {
        this.pos += skip;
    }

    public char current() {
        return array[pos];
    }

    public boolean hasNext() {
        return pos < length;
    }
}
